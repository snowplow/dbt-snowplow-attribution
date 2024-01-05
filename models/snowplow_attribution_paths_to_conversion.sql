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
    unique_key='event_id',
    upsert_date_key='cv_tstamp',
    sort='cv_tstamp',
    dist='event_id',
    partition_by = snowplow_utils.get_value_by_target_type(bigquery_val = {
      "field": "cv_tstamp",
      "data_type": "timestamp"
    }, databricks_val='cv_tstamp_date'),
    cluster_by=snowplow_utils.get_value_by_target_type(bigquery_val=["user_identifier","session_identifier"], snowflake_val=["to_date(cv_tstamp)"]),
    tags=["derived"],
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    tblproperties={
      'delta.autoOptimize.optimizeWrite' : 'true',
      'delta.autoOptimize.autoCompact' : 'true'
    },
    snowplow_optimize = true
  )
}}

{%- set __, last_processed_cv_tstamp = snowplow_utils.return_limits_from_model(this,'cv_path_start_tstamp','cv_path_start_tstamp') %}

{{ source_check() }}

-- Requires macro trim_long_path

with paths as (
  
  select
    {% if var('snowplow__conversion_stitching') %}
      stitched_user_id as customer_id,
    {% else %}
      case when p.user_id is not null and p.user_id != '' then p.user_id -- use event user_id
        else p.user_identifier end as customer_id,
    {% endif %}
    start_tstamp as visit_start_tstamp, -- we consider the event timestamp to be the session start, rather than the session start timestamp
    {{ channel_classification() }} as channel,
    refr_urlpath as referral_path,
    mkt_campaign as campaign,
    mkt_source as source,
    mkt_medium as medium

    {% if target.type in ['databricks', 'spark'] -%}
      , date(start_tstamp) as visit_start_date
    {%- endif %}

  from {{ var('snowplow__conversion_path_source') }} p

  where start_tstamp >= date '{{ var("snowplow_attribution_start_date") }}'

  {% if is_incremental() %}
    {% if target.type in ['databricks', 'spark'] -%}
      and visit_start_date >= date({{ snowplow_utils.timestamp_add('day', -var("snowplow__path_lookback_days", 30), last_processed_cv_tstamp) }})
    {% else %}
      and visit_start_tstamp >= {{ snowplow_utils.timestamp_add('day', -var("snowplow__path_lookback_days", 30), last_processed_cv_tstamp) }}
    {% endif %}
  {% endif %}

  {% if var('snowplow__conversion_hosts')|length > 0 %}
    -- restrict to certain hostnames
    and first_page_urlhost in ({{ snowplow_utils.print_list(var('snowplow__conversion_hosts')) }})
  {% endif %}
  
  {% if var('snowplow__consider_intrasession_channels') %}
    -- yields one row per channel change
    and mkt_medium is not null and mkt_medium != ''
  {% endif %}

)

, conversions as (
  
  select
    ev.event_id,
    
    {% if var('snowplow__conversion_stitching') %}
      -- updated with mapping as part of post hook on derived conversions table
      ev.stitched_user_id as customer_id,
    {% else %}
      case when ev.user_id is not null and ev.user_id != '' then ev.user_id
          else ev.user_identifier end as customer_id,
    {% endif %} 
    
    cv_tstamp,
    
    {{ conversion_value() }} as revenue
    
    {% if target.type in ['databricks', 'spark'] -%}
      , date(cv_tstamp) as cv_tstamp_date
    {%- endif %}

  from {{ var('snowplow__conversions_source' )}} as ev

  where ( {{ conversion_clause() }} )

  {% if is_incremental() %}
    {% if target.type in ['databricks', 'spark'] -%}
      and cv_tstamp_date >= date({{ snowplow_utils.timestamp_add('hour', -var("snowplow__lookback_window_hours", 6), last_processed_cv_tstamp) }})
    {% else %}
      and cv_tstamp >= {{ snowplow_utils.timestamp_add('hour', -var("snowplow__lookback_window_hours", 6), last_processed_cv_tstamp) }}
    {% endif %}
  {% endif %} 

)

, string_aggs as (
  
  select
    c.event_id,
    c.customer_id,
    c.cv_tstamp, 
    {% if target.type in ['databricks', 'spark'] -%}
      c.cv_tstamp_date,
    {%- endif %}
    {{ snowplow_utils.timestamp_add('day', -var("snowplow__path_lookback_days"), 'c.cv_tstamp') }} cv_path_start_tstamp,
    c.revenue,
    {{ snowplow_utils.get_string_agg('channel', 'p', separator=' > ', sort_numeric=false, order_by_column='visit_start_tstamp', order_by_column_prefix='p') }} as channel_path,
    {{ snowplow_utils.get_string_agg('campaign', 'p', separator=' > ', sort_numeric=false, order_by_column='visit_start_tstamp', order_by_column_prefix='p') }} as campaign_path
  
  from conversions c

  inner join paths p
  on c.customer_id = p.customer_id
   
  and {{ datediff('p.visit_start_tstamp', 'c.cv_tstamp', 'day') }} <= {{ var('snowplow__path_lookback_days') }}
  and {{ datediff('p.visit_start_tstamp', 'c.cv_tstamp', 'day') }}  >= 0
  
  where 1 = 1
  
  {% if var('snowplow__channels_to_exclude') %}
    -- Filters out any unwanted channels
    and channel not in ({{ snowplow_utils.print_list(var('snowplow__channels_to_exclude')) }})
  {% endif %}

  {% if var('snowplow__channels_to_include') %}
    -- Filters out any unwanted channels
    and channel in ({{ snowplow_utils.print_list(var('snowplow__channels_to_include')) }})
  {% endif %}
  
  {{ dbt_utils.group_by(n=5) }} {% if target.type in ['databricks', 'spark'] -%}, 6{% endif %}
)

 {% if target.type not in ('redshift') %}

, arrays as (

  select
    event_id,
    customer_id,
    cv_tstamp,
    {% if target.type in ['databricks', 'spark'] -%}
      cv_tstamp_date,
    {%- endif %}
    cv_path_start_tstamp,
    revenue,
    {{ snowplow_utils.get_split_to_array('channel_path', 's', ' > ') }} as channel_path,
    {{ snowplow_utils.get_split_to_array('channel_path', 's', ' > ') }} as channel_transformed_path,
    {{ snowplow_utils.get_split_to_array('campaign_path', 's', ' > ') }} as campaign_path,
    {{ snowplow_utils.get_split_to_array('campaign_path', 's', ' > ') }} as campaign_transformed_path
    
  from string_aggs s
)

{{ transform_paths('conversions', 'arrays') }}

select
  event_id,
  customer_id,
  cv_tstamp,
  cv_path_start_tstamp,
  revenue,
  {{ snowplow_utils.get_array_to_string('channel_path', 't', ' > ') }} as channel_path,
  {{ snowplow_utils.get_array_to_string('channel_transformed_path', 't', ' > ') }} as channel_transformed_path,
  {{ snowplow_utils.get_array_to_string('campaign_path', 't', ' > ') }} as campaign_path,
  {{ snowplow_utils.get_array_to_string('campaign_transformed_path', 't', ' > ') }} as campaign_transformed_path

from path_transforms t

{% else %}

, strings as (

  select
    event_id,
    customer_id,
    cv_tstamp,
    {% if target.type in ['databricks', 'spark'] -%}
      cv_tstamp_date,
    {%- endif %}
    cv_path_start_tstamp,
    revenue,
    channel_path as channel_path,
    channel_path as channel_transformed_path
    campaign_path as campaign_path,
    campaign_path as campaign_transformed_path

  from string_aggs s
)

  {{ transform_paths('conversions', 'strings') }}

select *
from path_transforms p

{% endif %}
