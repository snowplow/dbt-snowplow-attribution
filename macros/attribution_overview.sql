{#
Copyright (c) 2024-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% macro attribution_overview() %}
    {{ return(adapter.dispatch('attribution_overview', 'snowplow_attribution')()) }}
{% endmacro %}


{% macro default__attribution_overview() %}

{%- set __, last_processed_cv_tstamp = snowplow_utils.return_limits_from_model(ref('snowplow_attribution_campaign_attributions'),'cv_tstamp','cv_tstamp',true) %}

-- making sure we only include spend that is needed
with spend_with_unique_keys as (
  {% if var('snowplow__spend_source') != 'not defined' %}
    select row_number() over(order by spend_tstamp) as spend_id, *
    from {{ var('snowplow__spend_source') }}
  {% else %}
    select true
  {% endif %}
)

-- we need to dedupe as the join does the filtering, we can't group them upfront
, campaign_spend as (

  {% if var('snowplow__spend_source') != 'not defined' %}
    select s.campaign, s.spend
    from spend_with_unique_keys s
    inner join {{ ref('snowplow_attribution_campaign_attributions') }} c
    on c.campaign = s.campaign and s.spend_tstamp < cv_tstamp 
    and s.spend_tstamp > {{ snowplow_utils.timestamp_add('day', -90, 'cv_tstamp') }}
    where s.campaign is not null
    qualify row_number() over (partition by s.spend_id order by s.spend_tstamp) = 1
  
  {% else %}
    select true
  {% endif %}
    
)

, channel_spend as (
  
  {% if var('snowplow__spend_source') != 'not defined' %}
    select s.channel, s.spend
    from spend_with_unique_keys s
    inner join {{ ref('snowplow_attribution_channel_attributions') }} c
    on c.channel = s.channel and s.spend_tstamp < cv_tstamp
    and s.spend_tstamp > {{ snowplow_utils.timestamp_add('day', -90, 'cv_tstamp') }}
    where s.channel is not null
    qualify row_number() over (partition by s.spend_id order by s.spend_tstamp) = 1
  
  {% else %}
    select true
  {% endif %}
    
)

-- grouping spend to avoid duplicates in later join
, campaign_spend_grouped as (

  {% if var('snowplow__spend_source') != 'not defined' %}
    select campaign, sum(spend) as spend
    from campaign_spend
    group by 1
  
  {% else %}
    select true
  {% endif %}
    
)

, channel_spend_grouped as (
  
  {% if var('snowplow__spend_source') != 'not defined' %}
    select channel, sum(spend) as spend
    from channel_spend
    group by 1
  
  {% else %}
    select true
  {% endif %}
    
)


, campaign_prep as (
  
  select
    c.cv_id,
    c.event_id,
    c.campaign,
    c.cv_tstamp,
    c.cv_type,
    sum(c.first_touch_attribution) as first_touch_attribution,
    sum(c.last_touch_attribution) as last_touch_attribution,
    sum(c.linear_attribution) as linear_attribution,
    sum(c.position_based_attribution) as position_based_attribution,
    coalesce(min(c.cv_total_revenue),0) as cv_total_revenue,
    
    {% if var('snowplow__spend_source') != 'not defined' %}
      min(s.spend) as spend
    {% else %}
      cast(null as {{ dbt.type_numeric() }}) as spend
    {% endif %}
    
  from {{ ref('snowplow_attribution_campaign_attributions') }} c
  
  {% if var('snowplow__spend_source') != 'not defined' %}
    left join campaign_spend_grouped s
    on s.campaign = c.campaign
  {% endif %}
  
  where
  {% if not var('snowplow__conversion_window_start_date') == '' and not var('snowplow__conversion_window_end_date') == '' %}
    cv_tstamp >= '{{ var("snowplow__conversion_window_start_date") }}' and cv_tstamp < '{{ var("snowplow__conversion_window_end_date") }}'
  {% else %}
    cv_tstamp >= {{ snowplow_utils.timestamp_add('day', -var("snowplow__conversion_window_days"), last_processed_cv_tstamp) }}
  {% endif%}
  
  {{ dbt_utils.group_by(n=5) }}
)

, channel_prep as (
  
  select
    c.cv_id,
    c.event_id,
    c.cv_tstamp,
    c.cv_type,
    c.channel,
    sum(c.first_touch_attribution) as first_touch_attribution,
    sum(c.last_touch_attribution) as last_touch_attribution,
    sum(c.linear_attribution) as linear_attribution,
    sum(c.position_based_attribution) as position_based_attribution,
    coalesce(min(c.cv_total_revenue),0) as cv_total_revenue,
  
  {% if var('snowplow__spend_source') != 'not defined' %}
    min(s.spend) as spend
  {% else %}
    cast(null as {{ dbt.type_numeric() }}) as spend
  {% endif %}
  
  from {{ ref('snowplow_attribution_channel_attributions') }} c
  
  {% if var('snowplow__spend_source') != 'not defined' %}
    left join channel_spend_grouped s
    on s.channel = c.channel
  {% endif %}
  
  where 
  
  {% if not var("snowplow__conversion_window_start_date") == '' and not var("snowplow__conversion_window_end_date") == '' %}
    cv_tstamp >= '{{ var("snowplow__conversion_window_start_date") }}' and cv_tstamp < '{{ var("snowplow__conversion_window_end_date") }}'
  {% else %}
    cv_tstamp >= {{ snowplow_utils.timestamp_add('day', -var("snowplow__conversion_window_days"), last_processed_cv_tstamp) }}
  {% endif %}
  
  {{ dbt_utils.group_by(n=5) }}
)

, unions as (
  
  {% set attribution_list = var('snowplow__attribution_list') %}
  
  {% for attribution in attribution_list %}
    select
      'channel' as path_type,
      '{{ attribution }}' as attribution_type,
      channel as touch_point,
      count(distinct cv_id) as in_n_conversion_paths,
      (sum({{ attribution }}_attribution)/sum(cv_total_revenue))*count(distinct cv_id)  as attributed_conversions,
      min(cv_tstamp) as min_cv_tstamp,
      max(cv_tstamp) as max_cv_tstamp,
      min(spend) as spend,
      sum(cv_total_revenue) as sum_cv_total_revenue,
      sum({{ attribution }}_attribution) as attributed_revenue
      
    from channel_prep
    
    {{ dbt_utils.group_by(n=3) }}
    
    union all
  {% endfor %}
   
  {% for attribution in attribution_list %}
    select
      'campaign' as path_type,
      '{{ attribution }}' as attribution_type,
      campaign as touch_point,
      count(distinct cv_id) as in_n_conversion_paths,
      (sum({{ attribution }}_attribution)/sum(cv_total_revenue))*count(distinct cv_id)  as attributed_conversions,
      min(cv_tstamp) as min_cv_tstamp,
      max(cv_tstamp) as max_cv_tstamp,
      min(spend) as spend,
      sum(cv_total_revenue) as sum_cv_total_revenue,
      sum({{ attribution }}_attribution) as attributed_revenue
      
    from campaign_prep
    
    {{ dbt_utils.group_by(n=3) }}
    
    {%- if not loop.last %}
      union all
    {% endif %}
  {% endfor %}
  
)

select
  path_type,
  attribution_type,
  touch_point,
  in_n_conversion_paths,
  attributed_conversions,
  min_cv_tstamp,
  max_cv_tstamp,
  spend,
  sum_cv_total_revenue,
  attributed_revenue
  {% if var('snowplow__spend_source') != 'not defined' %}
    , sum(attributed_revenue) / nullif(sum(spend), 0) as ROAS
  {% endif %}

from unions

where touch_point is not null

{{ dbt_utils.group_by(n=10) }}

order by 1,2,3
  
{% endmacro %}
