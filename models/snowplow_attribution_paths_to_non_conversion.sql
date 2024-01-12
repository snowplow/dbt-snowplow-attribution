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
    pre_hook= "{{ source_checks() }}",
    unique_key='surrogate_key',
    upsert_date_key='non_cv_date',
    sort='non_cv_date',
    dist='surrogate_key',
    partition_by = snowplow_utils.get_value_by_target_type(bigquery_val = {
      "field": "non_cv_date",
      "data_type": "timestamp"
    }, databricks_val='non_cv_date'),
    cluster_by=snowplow_utils.get_value_by_target_type(bigquery_val=["user_identifier","session_identifier"], snowflake_val=["non_cv_date"]),
    tags=["derived"],
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    tblproperties={
      'delta.autoOptimize.optimizeWrite' : 'true',
      'delta.autoOptimize.autoCompact' : 'true'
    },
    snowplow_optimize = true
  )
}}

{%- set __, last_processed_cv_tstamp = snowplow_utils.return_limits_from_model(this,'non_cv_path_start_date','non_cv_path_start_date') %}

  with paths as (
    
    select
      {% if var('snowplow__conversion_stitching') %}
        stitched_user_id as customer_id,
      {% else %}
        case when p.user_id is not null and p.user_id != '' then p.user_id -- use event user_id
          else p.user_identifier end as customer_id,
      {% endif %}
      derived_tstamp as visit_start_tstamp, -- we consider the event timestamp to be the session start, rather than the session start timestamp
      {{ channel_classification() }}  as channel,
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
      -- restrict to certain hostnames, change it to first_page_urlhost if snowplow_unified_sessions is used as a path base instead of the defaulted snowplow_unified_views 
      and page_urlhost in ({{ snowplow_utils.print_list(var('snowplow__conversion_hosts')) }})
    {% endif %}
    
    {% if var('snowplow__consider_intrasession_channels') %}
      -- yields one row per channel change
      and ((mkt_medium is not null and mkt_medium != '') or view_in_session_index = 1)
    {% endif %}

  ), 
  
  non_conversions as (
    select
      s.customer_id,
      visit_start_tstamp as non_cv_tstamp,
      s.channel,
      s.campaign,
      c.event_id,
      {# This generates a unique number per "block" of non-cv events, and allows us to keep a block that are caught by a conversion path distinct from each other #}
      SUM(CASE WHEN event_id IS NOT NULL THEN 1 ELSE 0 END) over (partition by s.customer_id order by visit_start_tstamp asc rows BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as non_cv_block
  from
      paths s
  left join
        {{ ref('snowplow_attribution_paths_to_conversion') }} c
      on
        s.customer_id = c.customer_id
        and s.visit_start_tstamp >= c.cv_path_start_tstamp
        and s.visit_start_tstamp <= c.cv_tstamp
)

  , string_aggs as (
    select 
        l.customer_id, 
        l.non_cv_block,
        l.non_cv_tstamp,
        min(r.non_cv_tstamp) as non_cv_path_start_tstamp,
        {{ snowplow_utils.get_string_agg('channel', 'r', separator=' > ', sort_numeric=false, order_by_column='non_cv_tstamp', order_by_column_prefix='r') }} as channel,
        {{ snowplow_utils.get_string_agg('campaign', 'r', separator=' > ', sort_numeric=false, order_by_column='non_cv_tstamp', order_by_column_prefix='r') }} as campaign
    from
        non_conversions l
        left join non_conversions r on 
            l.customer_id = r.customer_id
            and {{ datediff('r.non_cv_tstamp', 'l.non_cv_tstamp', 'day') }} <= {{ var('snowplow__path_lookback_days') }}
            and r.non_cv_tstamp <= l.non_cv_tstamp
    where 
        -- ignore any touch points that are captured by a conversion window
        l.event_id is null
        {% if var('snowplow__channels_to_exclude') %}
          -- Filters out any unwanted channels
          and channel not in ({{ snowplow_utils.print_list(var('snowplow__channels_to_exclude')) }})
        {% endif %}

        {% if var('snowplow__channels_to_include') %}
          -- Filters out any unwanted channels
          and channel in ({{ snowplow_utils.print_list(var('snowplow__channels_to_include')) }})
        {% endif %}
    {{ dbt_utils.group_by(n=3) }}
  )

  , arrays as (

    select
      customer_id,
      non_cv_tstamp,
      non_cv_path_start_tstanon_cv_tstamp,
      {{ snowplow_utils.get_split_to_array('channel', 's', ' > ') }} as channel_path,
      {{ snowplow_utils.get_split_to_array('channel', 's', ' > ') }} as channel_transformed_path,
      {{ snowplow_utils.get_split_to_array('campaign', 's', ' > ') }} as campaign_path,
      {{ snowplow_utils.get_split_to_array('campaign', 's', ' > ') }} as campaign_transformed_path
      
    from string_aggs s
  )

  {{ transform_paths('non_conversions', 'arrays') }}

  select
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'non_cv_tstamp', 'non_cv_path_start_tstamp'])}} as surrogate_key,
    customer_id,
    non_cv_tstamp,
    non_cv_path_start_tstamp,
    {{ snowplow_utils.get_array_to_string('channel_path', 't', ' > ') }} as channel_path,
    {{ snowplow_utils.get_array_to_string('channel_transformed_path', 't', ' > ') }} as channel_transformed_path,
    {{ snowplow_utils.get_array_to_string('campaign_path', 't', ' > ') }} as campaign_path,
    {{ snowplow_utils.get_array_to_string('campaign_transformed_path', 't', ' > ') }} as campaign_transformed_path

  from path_transforms t
