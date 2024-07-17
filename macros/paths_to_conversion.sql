{#
Copyright (c) 2024-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}


/* Macro to create the sql for the first incremental model of the package, the paths_to_conversion
   If you are not using the default path (snowplow_unified.views) and conversion sources (snowplow_unified.conversions)
   please make sure to align the source field names */

-- Requires macro trim_long_path


{% macro paths_to_conversion() %}
    {{ return(adapter.dispatch('paths_to_conversion', 'snowplow_attribution')()) }}
{% endmacro %}

{% macro default__paths_to_conversion() %}

{%- set __, last_processed_cv_tstamp = snowplow_utils.return_limits_from_model(this,'cv_path_start_tstamp','cv_path_start_tstamp') %}

  with paths as (
    
    select
      {% if var('snowplow__conversion_stitching', false) %}
        stitched_user_id as customer_id,
      {% else %}
        coalesce(um.user_id, p.user_identifier) as customer_id,
      {% endif %}
      derived_tstamp as visit_start_tstamp, -- we consider the event timestamp to be the session start, rather than the session start timestamp
      {{ channel_classification() }} as channel,
      refr_urlpath as referral_path,
      case when mkt_campaign is null then 'No campaign' when mkt_campaign = '' then 'No campaign' else mkt_campaign end as campaign,
      mkt_source as source,
      mkt_medium as medium

      {% if target.type in ['databricks', 'spark'] -%}
        , date(start_tstamp) as visit_start_date
      {%- endif %}

    from {{ var('snowplow__conversion_path_source') }} p
    
    {% if not var('snowplow__conversion_stitching', false) %}
      left join {{ var('snowplow__user_mapping_source') }} um
      on um.user_identifier = p.user_identifier
    {% endif %}

    where start_tstamp >= timestamp '{{ var("snowplow__attribution_start_date") }}'
    
    and p.user_identifier is not null

    {% if is_incremental() %}
        and derived_tstamp >= {{ snowplow_utils.timestamp_add('day', -var("snowplow__path_lookback_days", 30), last_processed_cv_tstamp) }}
    {% endif %}

    {% if var('snowplow__conversion_hosts') %}
      -- restrict to certain hostnames, change it to first_page_urlhost if snowplow_unified_sessions is used as a path base instead of the defaulted snowplow_unified_views 
      and page_urlhost in ({{ snowplow_utils.print_list(var('snowplow__conversion_hosts')) }})
    {% endif %}
    
    {% if var('snowplow__consider_intrasession_channels') %}
      -- yields one row per channel change
      and ((mkt_medium is not null and mkt_medium != '') or view_in_session_index = 1)
    {% endif %}

  )

  , conversions as (
    
    select
      ev.cv_id,
      ev.event_id,
      
      {% if var('snowplow__conversion_stitching', false) %}
        -- updated with mapping as part of post hook on derived conversions table
        ev.stitched_user_id as customer_id,
      {% else %}
        coalesce(um.user_id, ev.user_identifier) as customer_id,
      {% endif %} 
      
      ev.cv_tstamp,
      ev.cv_type,
      ev.cv_value as revenue
  
    from {{ var('snowplow__conversions_source' )}} as ev
    
    {% if not var('snowplow__conversion_stitching', false) %}
      left join {{ var('snowplow__user_mapping_source') }} um
      on um.user_identifier = ev.user_identifier
    {% endif %}

    where 
    
    {{ var('snowplow__conversion_clause') }} 

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
      c.cv_id,
      c.event_id,
      c.customer_id,
      c.cv_tstamp, 
      c.cv_type,
      {{ snowplow_utils.timestamp_add('day', -var("snowplow__path_lookback_days"), 'c.cv_tstamp') }} cv_path_start_tstamp,
      c.revenue,
      {{ snowplow_utils.get_string_agg('channel', 'p', separator=' > ', sort_numeric=false, order_by_column='visit_start_tstamp', order_by_column_prefix='p') }} as channel,
      {{ snowplow_utils.get_string_agg('campaign', 'p', separator=' > ', sort_numeric=false, order_by_column='visit_start_tstamp', order_by_column_prefix='p') }} as campaign
    
    from conversions c

    inner join paths p
    on c.customer_id = p.customer_id
    
    and {{ datediff('p.visit_start_tstamp', 'c.cv_tstamp', 'day') }} <= {{ var('snowplow__path_lookback_days') }}
    and visit_start_tstamp <= cv_tstamp
    
    where 1 = 1
    
    {% if var('snowplow__channels_to_exclude') %}
      -- Filters out any unwanted channels
      and channel not in ({{ snowplow_utils.print_list(var('snowplow__channels_to_exclude')) }})
    {% endif %}

    {% if var('snowplow__channels_to_include') %}
      -- Filters out any unwanted channels
      and channel in ({{ snowplow_utils.print_list(var('snowplow__channels_to_include')) }})
    {% endif %}
    
    {% if var('snowplow__campaigns_to_exclude') %}
      -- Filters out any unwanted channels
      and (campaign not in ({{ snowplow_utils.print_list(var('snowplow__campaigns_to_exclude')) }}) or campaign is null)
    {% endif %}

    {% if var('snowplow__campaigns_to_include') %}
      -- Filters out any unwanted channels
      and campaign in ({{ snowplow_utils.print_list(var('snowplow__campaigns_to_include')) }})
    {% endif %}
    
    {{ dbt_utils.group_by(n=7) }}
  )

  , arrays as (

    select
      cv_id,
      event_id,
      customer_id,
      cv_tstamp,
      cv_type,
      cv_path_start_tstamp,
      revenue,
      {{ snowplow_utils.get_split_to_array('channel', 's', ' > ') }} as channel_path,
      {{ snowplow_utils.get_split_to_array('channel', 's', ' > ') }} as channel_transformed_path,
      {{ snowplow_utils.get_split_to_array('campaign', 's', ' > ') }} as campaign_path,
      {{ snowplow_utils.get_split_to_array('campaign', 's', ' > ') }} as campaign_transformed_path
      
    from string_aggs s
  )

  {{ transform_paths('conversions', 'arrays') }}

  select
    cv_id,
    event_id,
    customer_id,
    cv_tstamp,
    cv_type,
    {% if target.type in ['databricks', 'spark'] -%}
      date(cv_tstamp) as cv_tstamp_date,
    {%- endif %}
    cv_path_start_tstamp,
    revenue,
    {{ snowplow_utils.get_array_to_string('channel_path', 't', ' > ') }} as channel_path,
    {{ snowplow_utils.get_array_to_string('channel_transformed_path', 't', ' > ') }} as channel_transformed_path,
    {{ snowplow_utils.get_array_to_string('campaign_path', 't', ' > ') }} as campaign_path,
    {{ snowplow_utils.get_array_to_string('campaign_transformed_path', 't', ' > ') }} as campaign_transformed_path

  from path_transforms t


{% endmacro %}

{% macro redshift__paths_to_conversion() %}

{%- set __, last_processed_cv_tstamp = snowplow_utils.return_limits_from_model(this,'cv_path_start_tstamp','cv_path_start_tstamp') %}

  with paths as (
    
    select
      {% if var('snowplow__conversion_stitching', false) %}
        stitched_user_id as customer_id,
      {% else %}
        coalesce(um.user_id, p.user_identifier) as customer_id,
      {% endif %}
      derived_tstamp as visit_start_tstamp, -- we consider the event timestamp to be the session start, rather than the session start timestamp
      {{ channel_classification() }} as channel,
      refr_urlpath as referral_path,
      case when mkt_campaign is null then 'No campaign' when mkt_campaign = '' then 'No campaign' else mkt_campaign end as campaign,
      mkt_source as source,
      mkt_medium as medium

      {% if target.type in ['databricks', 'spark'] -%}
        , date(start_tstamp) as visit_start_date
      {%- endif %}

    from {{ var('snowplow__conversion_path_source') }} p
    
    {% if not var('snowplow__conversion_stitching', false) %}
      left join {{ var('snowplow__user_mapping_source') }} um
      on um.user_identifier = p.user_identifier
    {% endif %}

    where start_tstamp >= date '{{ var("snowplow__attribution_start_date") }}'
    
    and p.user_identifier is not null

    {% if is_incremental() %}
      and derived_tstamp >= {{ snowplow_utils.timestamp_add('day', -var("snowplow__path_lookback_days", 30), last_processed_cv_tstamp) }}
    {% endif %}

    {% if var('snowplow__conversion_hosts') != [] %}
      -- restrict to certain hostnames, change it to first_page_urlhost if snowplow_unified_sessions is used as a path base instead of the defaulted snowplow_unified_views 
      and page_urlhost in ({{ snowplow_utils.print_list(var('snowplow__conversion_hosts')) }})
    {% endif %}
    
    {% if var('snowplow__consider_intrasession_channels') %}
      -- yields one row per channel change
      and ((mkt_medium is not null and mkt_medium != '') or view_in_session_index = 1)
    {% endif %}

  )

  , conversions as (
    
    select
      ev.cv_id,
      ev.event_id,
      
      {% if var('snowplow__conversion_stitching', false) %}
        -- updated with mapping as part of post hook on derived conversions table
        ev.stitched_user_id as customer_id,
      {% else %}
        coalesce(um.user_id, ev.user_identifier) as customer_id,
      {% endif %} 
      
      ev.cv_tstamp,
      ev.cv_type,
      ev.cv_value as revenue

    from {{ var('snowplow__conversions_source' )}} as ev
    
    {% if not var('snowplow__conversion_stitching', false) %}
      left join {{ var('snowplow__user_mapping_source') }} um
      on um.user_identifier = ev.user_identifier
    {% endif %}

    where {{ var('snowplow__conversion_clause') }} 

    {% if is_incremental() %}
      and cv_tstamp >= {{ snowplow_utils.timestamp_add('hour', -var("snowplow__lookback_window_hours", 6), last_processed_cv_tstamp) }}
    {% endif %} 

  )

  , string_aggs as (
    
    select
      c.cv_id,
      c.event_id,
      c.customer_id,
      c.cv_tstamp, 
      c.cv_type,
      {{ snowplow_utils.timestamp_add('day', -var("snowplow__path_lookback_days"), 'c.cv_tstamp') }} cv_path_start_tstamp,
      c.revenue,
      {{ snowplow_utils.get_string_agg('channel', 'p', separator=' > ', sort_numeric=false, order_by_column='visit_start_tstamp', order_by_column_prefix='p') }} as channel,
      {{ snowplow_utils.get_string_agg('campaign', 'p', separator=' > ', sort_numeric=false, order_by_column='visit_start_tstamp', order_by_column_prefix='p') }} as campaign
    
    from conversions c

    inner join paths p
    on c.customer_id = p.customer_id
    
    and {{ datediff('p.visit_start_tstamp', 'c.cv_tstamp', 'day') }} <= {{ var('snowplow__path_lookback_days') }}
    and {{ datediff('p.visit_start_tstamp', 'c.cv_tstamp', 'day') }}  >= 0
    
    where 1 = 1
    
    {% if var('snowplow__channels_to_exclude') != [] %}
      -- Filters out any unwanted channels
      and (channel not in ({{ snowplow_utils.print_list(var('snowplow__channels_to_exclude')) }}) or channel is null)
    {% endif %}

    {% if var('snowplow__channels_to_include') != [] %}
      -- Filters out any unwanted channels
      and channel in ({{ snowplow_utils.print_list(var('snowplow__channels_to_include')) }})
    {% endif %}

    {% if var('snowplow__campaigns_to_exclude') != [] %}
      -- Filters out any unwanted channels
      and (campaign not in ({{ snowplow_utils.print_list(var('snowplow__campaigns_to_exclude')) }}) or campaign is null)
    {% endif %}

    {% if var('snowplow__campaigns_to_include') != [] %}
      -- Filters out any unwanted channels
      and campaign in ({{ snowplow_utils.print_list(var('snowplow__campaigns_to_include')) }})
    {% endif %}
    
    {{ dbt_utils.group_by(n=7) }}
  )

  , strings as (

    select
      cv_id,
      event_id,
      customer_id,
      cv_tstamp,
      cv_type,
      cv_path_start_tstamp,
      revenue,
      channel as channel_path,
      channel as channel_transformed_path,
      campaign as campaign_path,
      campaign as campaign_transformed_path

    from string_aggs s
  )

    {{ transform_paths('conversions', 'strings') }}

  select *
  from path_transforms p

{% endmacro %}
