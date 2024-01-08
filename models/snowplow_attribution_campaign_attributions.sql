{#
Copyright (c) 2024-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{{
  config(
    materialized='incremental',
    full_refresh=snowplow_attribution.allow_refresh(),
    on_schema_change='append_new_columns',
    unique_key='composite_key',
    upsert_date_key='cv_tstamp',
    sort='cv_tstamp',
    dist='composite_key',
    partition_by = snowplow_utils.get_value_by_target_type(bigquery_val = {
      "field": "cv_tstamp",
      "data_type": "timestamp"
    }, databricks_val='cv_tstamp_date'),
    cluster_by=snowplow_utils.get_value_by_target_type(bigquery_val=["event_id","customer_id"], snowflake_val=["to_date(cv_tstamp)"]),
    tags=["derived"],
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    tblproperties={
      'delta.autoOptimize.optimizeWrite' : 'true',
      'delta.autoOptimize.autoCompact' : 'true'
    },
    snowplow_optimize = true
  )
}}

{%- set __, last_processed_cv_tstamp = snowplow_utils.return_limits_from_model(ref('snowplow_attribution_incremental_manifest'),'last_processed_cv_tstamp','last_processed_cv_tstamp') %}


with arrays as (
  
  select
    c.event_id,
    c.customer_id,
    c.cv_tstamp,
    c.revenue,
    c.campaign_transformed_path,
    {{ snowplow_utils.get_split_to_array('campaign_transformed_path', 'c', ' > ') }} as campaign_path_array
    
  from {{ ref('snowplow_attribution_paths_to_conversion') }} c

  {% if is_incremental() %}
    {% if target.type in ['databricks', 'spark'] -%}
      where cv_tstamp_date >= date({{ snowplow_utils.timestamp_add('day', -var("snowplow__path_lookback_days", 30), last_processed_cv_tstamp) }})
    {% else %}
      where cv_tstamp >= {{ snowplow_utils.timestamp_add('day', -var("snowplow__path_lookback_days", 30), last_processed_cv_tstamp) }}
    {% endif %}
  {% endif %}
  
)

, unnesting as (
  {{ snowplow_utils.unnest('event_id', 'campaign_path_array', 'campaign', 'arrays', with_index=true) }}
)

, prep as (
  
  select 
    a.event_id,
    a.customer_id,
    a.cv_tstamp,
    a.revenue,
    a.campaign_transformed_path,
    u.campaign,
    u.source_index,
    {{ snowplow_utils.get_array_size('campaign_path_array') }} as path_length,
    case when u.source_index = max(u.source_index) over (partition by u.event_id) then true else false end as is_last_element,
    case when {{ snowplow_utils.get_array_size('campaign_path_array') }} = 1 then revenue
        when {{ snowplow_utils.get_array_size('campaign_path_array') }} = 2 then revenue/2
        else null end as position_based_attribution
  from arrays a

left join unnesting u
on a.event_id = u.event_id

)

select
  event_id || campaign || source_index as composite_key,
  event_id,
  customer_id,
  cv_tstamp,
  revenue as conversion_total_revenue,
  campaign_transformed_path,
  campaign,
  source_index,
  path_length,
  case when source_index = 0 then revenue else 0 end as first_touch_attribution,
  case when is_last_element then revenue else 0 end as last_touch_attribution,
  round(revenue / nullif(path_length, 0)) as linear_attribution,
  round(case when position_based_attribution is not null then position_based_attribution
      when source_index = 0 then revenue * 0.4
      when is_last_element then revenue * 0.4
      else revenue * 0.2 / path_length-2 end) as position_based_attribution

from prep


