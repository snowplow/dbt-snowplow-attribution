{#
Copyright (c) 2024-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}


{{
  config(
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    enabled=var('snowplow__enable_paths_to_non_conversion')
  )
}}

-- Requires macro trim_long_path

with paths as (
  
  select
    {% if var('snowplow__conversion_stitching') %}
      stitched_user_id as customer_id,
    {% else %}
      case when p.user_id is not null and p.user_id != '' then p.user_id -- use event user_id
        else p.user_identifier end as customer_id,
    {% endif %}
    derived_tstamp as visit_start_tstamp, -- we consider the event timestamp to be the session start, rather than the session start timestamp
    {{ channel_classification() }} as channel,
    refr_urlpath as referral_path,
    mkt_campaign as campaign,
    mkt_source as source,
    mkt_medium as medium

    {% if target.type in ['databricks', 'spark'] -%}
      , date(start_tstamp) as visit_start_date
    {%- endif %}

  from {{ var('snowplow__conversion_path_source') }} p

  where start_tstamp >= '{{ var("snowplow_attribution_start_date") }}'

  {% if var('snowplow__conversion_hosts')|length > 0 %}
    -- restrict to certain hostnames
    and page_urlhost in ({{ snowplow_utils.print_list(var('snowplow__conversion_hosts')) }})
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
      
      ev.cv_tstamp,
      ev.cv_value as revenue
      
      {% if target.type in ['databricks', 'spark'] -%}
        , date(cv_tstamp) as cv_tstamp_date
      {%- endif %}

    from {{ var('snowplow__conversions_source' )}} as ev

    where cv_value > 0 and cv_tstamp >= '{{ var("snowplow_attribution_start_date") }}'
)

, non_conversions as (

  select
    customer_id,
    max(visit_start_tstamp) as non_cv_tstamp

  from paths s

  where not exists (select customer_id from conversions c where s.customer_id = c.customer_id)

  
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

  group by 1

)

, string_aggs as (

  select
    n.customer_id,
    {{ snowplow_utils.get_string_agg('channel', 'p', separator=' > ', sort_numeric=false, order_by_column='visit_start_tstamp', order_by_column_prefix='p') }} as channel,
    {{ snowplow_utils.get_string_agg('campaign', 'p', separator=' > ', sort_numeric=false, order_by_column='visit_start_tstamp', order_by_column_prefix='p') }} as campaign
  
  from non_conversions n

  inner join paths p 
  on n.customer_id = p.customer_id
  
  and {{ datediff('p.visit_start_tstamp', 'n.non_cv_tstamp', 'day') }} <= {{ var('snowplow__path_lookback_days') }}
  and {{ datediff('p.visit_start_tstamp', 'n.non_cv_tstamp', 'day') }}  >= 0
  
  where 1 = 1
  
  {% if var('snowplow__channels_to_exclude') %}
    -- Filters out any unwanted channels
    and channel not in ({{ snowplow_utils.print_list(var('snowplow__channels_to_exclude')) }})
  {% endif %}

  {% if var('snowplow__channels_to_include') %}
    -- Filters out any unwanted channels
    and channel in ({{ snowplow_utils.print_list(var('snowplow__channels_to_include')) }})
  {% endif %}
  
  group by 1
)

{% if target.type not in ('redshift') %}

, arrays as (

    select
      customer_id,
      {{ snowplow_utils.get_split_to_array('channel', 's', ' > ') }} as channel_path,
      {{ snowplow_utils.get_split_to_array('channel', 's', ' > ') }} as channel_transformed_path,
      {{ snowplow_utils.get_split_to_array('campaign', 's', ' > ') }} as campaign_path,
      {{ snowplow_utils.get_split_to_array('campaign', 's', ' > ') }} as campaign_transformed_path

    from string_aggs s

)

{{ transform_paths('non_conversions', 'arrays') }}

select
  customer_id,
    {{ snowplow_utils.get_array_to_string('channel_path', 't', ' > ') }} as channel_path,
    {{ snowplow_utils.get_array_to_string('channel_transformed_path', 't', ' > ') }} as channel_transformed_path,
    {{ snowplow_utils.get_array_to_string('campaign_path', 't', ' > ') }} as campaign_path,
    {{ snowplow_utils.get_array_to_string('campaign_transformed_path', 't', ' > ') }} as campaign_transformed_path

from path_transforms t

{% else %}

, strings as (

  select
    customer_id,
    channel as channel,
    channel as channel_transformed_path
    campaign as campaign,
    campaign as campaign_transformed_path

  from string_aggs s

)

  {{ transform_paths('non_conversions', 'strings') }}


select *
from path_transforms t

{% endif %}
