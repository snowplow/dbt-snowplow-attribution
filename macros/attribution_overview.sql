{% macro attribution_overview() %}
    {{ return(adapter.dispatch('attribution_overview', 'snowplow_attribution')()) }}
{% endmacro %}


{% macro default__attribution_overview() %}

{%- set __, last_processed_cv_tstamp = snowplow_utils.return_limits_from_model(ref('snowplow_attribution_campaign_attributions'),'cv_tstamp','cv_tstamp') %}

with campaign_prep as (
  
  select
    c.event_id,
    c.campaign,
    c.cv_tstamp,
    sum(c.first_touch_attribution) as first_touch_attribution,
    sum(c.last_touch_attribution) as last_touch_attribution,
    sum(c.linear_attribution) as linear_attribution,
    sum(c.position_based_attribution) as position_based_attribution,
    coalesce(min(c.cv_total_revenue),0) as cv_total_revenue,
    
    {% if var('snowplow__spend_source') != 'not defined' %}
      min(s.spend) as spend
    {% else %}
      null as spend
    {% endif %}
    
  from {{ ref('snowplow_attribution_campaign_attributions') }} c
  
  {% if var('snowplow__spend_source') != 'not defined' %}
    left join {{ var('snowplow__spend_source') }} s
    on c.campaign = s.campaign and s.spend_timestamp < cv_tstamp 
    and s.spend_timestamp > {{ snowplow_utils.timestamp_add('day', -90, 'cv_tstamp') }}
  {% endif %}
  
  where
  {% if not var('snowplow__conversion_window_start_date') == '' and not var('snowplow__conversion_window_end_date') == '' %}
    cv_tstamp >= {{ var('snowplow__conversion_window_start_date') }} and cv_tstamp < {{ var('snowplow__conversion_window_end_date') }}
  {% else %}
    cv_tstamp >= {{ snowplow_utils.timestamp_add('day', -var("snowplow__conversion_window_days"), last_processed_cv_tstamp) }}
  {% endif%}
  
  {{ dbt_utils.group_by(n=3) }}
)

, channel_prep as (
  
  select
    c.event_id,
    c.cv_tstamp,
    c.channel,
    sum(c.first_touch_attribution) as first_touch_attribution,
    sum(c.last_touch_attribution) as last_touch_attribution,
    sum(c.linear_attribution) as linear_attribution,
    sum(c.position_based_attribution) as position_based_attribution,
    coalesce(min(c.cv_total_revenue),0) as cv_total_revenue,
  
  {% if var('snowplow__spend_source') != 'not defined' %}
    min(s.spend) as spend
  {% else %}
    null as spend
  {% endif %}
  
  from {{ ref('snowplow_attribution_channel_attributions') }} c
  
  {% if var('snowplow__spend_source') != 'not defined' %}
    left join {{ var('snowplow__spend_source') }} s
    on c.channel = s.channel and s.spend_timestamp < cv_tstamp 
    and s.spend_timestamp > {{ snowplow_utils.timestamp_add('day', -90, 'cv_tstamp') }}
  {% endif %}
  
  where 
  
  {% if not var("snowplow__conversion_window_start_date") == '' and not var("snowplow__conversion_window_end_date") == '' %}
    cv_tstamp >= {{ var('snowplow__conversion_window_start_date') }} and cv_tstamp < {{ var('snowplow__conversion_window_end_date') }}
  {% else %}
    cv_tstamp >= {{ snowplow_utils.timestamp_add('day', -var("snowplow__conversion_window_days"), last_processed_cv_tstamp) }}
  {% endif %}
  
  {{ dbt_utils.group_by(n=3) }}
)

, unions as (
  
  {% set attribution_list = var('snowplow__attribution_list') %}
  
  {% for attribution in attribution_list %}
    select
      'channel' as path_type,
      '{{ attribution }}' as attribution_type,
      channel as touch_point,
      count(distinct event_id) as in_n_conversion_paths,
      sum({{ attribution }}_attribution)/sum(cv_total_revenue) as attributed_conversions,
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
      count(distinct event_id) as in_n_conversion_paths,
      sum({{ attribution }}_attribution)/sum(cv_total_revenue) as attributed_conversions,
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
  *

  {% if var('snowplow__spend_source') != 'not defined' %}
    , sum(attributed_revenue) / nullif(sum(spend), 0) as ROAS
  {% endif %}

from unions

where touch_point is not null

{{ dbt_utils.group_by(n=10) }}

order by 1,2,3
  
{% endmacro %}
