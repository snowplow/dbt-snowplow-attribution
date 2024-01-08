{% macro overview() %}
    {{ return(adapter.dispatch('overview', 'snowplow_attribution')()) }}
{% endmacro %}


{% macro default__overview() %}

with campaign_prep as (
  
  select
    c.event_id,
    c.campaign,
    c.cv_tstamp,
    c.first_touch_attribution,
    c.last_touch_attribution,
    c.linear_attribution,
    c.position_based_attribution,
    coalesce(min(c.conversion_total_revenue),0) as conversion_total_revenue,
    
    {% if var('snowplow__spend_source')!="{{ source('atomic', 'events') }}" %}
      coalesce(min(s.spend),0) as spend
    {% else %}
      0 as spend
    {% endif %}
    
  from {{ ref('snowplow_attribution_campaign_attributions') }} c
  
  {% if var('snowplow__spend_source')!="{{ source('atomic', 'events') }}" %}
    left join {{ var('snowplow__spend_source') }} s
    on c.campaign = s.path and s.period < cv_tstamp 
    and s.period > {{ dbt.dateadd('day', -90, 'cv_tstamp') }}
  {% endif %}
  
  -- where cv_tstamp >= '2023-01-01' and cv_tstamp < '2023-03-01'
  
  {{ dbt_utils.group_by(n=7) }}
)

, channel_prep as (
  
  select
    c.event_id,
    c.cv_tstamp,
    c.channel,
    c.first_touch_attribution,
    c.last_touch_attribution,
    c.linear_attribution,
    c.position_based_attribution,
    coalesce(min(c.conversion_total_revenue),0) as conversion_total_revenue,
  
  {% if var('snowplow__spend_source')!="{{ source('atomic', 'events') }}" %}
    coalesce(min(s.spend),0) as spend
  {% else %}
    0 as spend
  {% endif %}
  
  from {{ ref('snowplow_attribution_channel_attributions') }} c
  
  {% if var('snowplow__spend_source')!="{{ source('atomic', 'events') }}" %}
    left join {{ var('snowplow__spend_source') }} s
    on c.channel = s.path and s.period < cv_tstamp 
    and s.period > {{ dbt.dateadd('day', -90, 'cv_tstamp') }}
  {% endif %}
  
  -- where cv_tstamp >= '2023-01-01' and cv_tstamp < '2023-03-01'

  {{ dbt_utils.group_by(n=7) }}
)

, unions as (
  
  select
    'channel' as path_type,
    'first_touch' as attribution_type,
    channel as touch_point,
    count(distinct event_id) as n_conversions,
    min(cv_tstamp) as min_cv_tstamp,
    max(cv_tstamp) as max_cv_tstamp,
    min(spend) as spend,
    sum(conversion_total_revenue) as sum_conversion_total_revenue,
    sum(first_touch_attribution) as attributed_revenue
    
  from channel_prep
  
  {{ dbt_utils.group_by(n=3) }}
  
  union all
  
    select
    'channel' as path_type,
    'last_touch' as attribution_type,
    channel as touch_point,
    count(distinct event_id) as n_conversions,
    min(cv_tstamp) as min_cv_tstamp,
    max(cv_tstamp) as max_cv_tstamp,
    min(spend) as spend,
    sum(conversion_total_revenue) as sum_conversion_total_revenue,
    sum(last_touch_attribution) as attributed_revenue
    
  from channel_prep
  
  {{ dbt_utils.group_by(n=3) }}

  union all
  
    select
    'channel' as path_type,
    'linear' as attribution_type,
    channel as touch_point,
    count(distinct event_id) as n_conversions,
    min(cv_tstamp) as min_cv_tstamp,
    max(cv_tstamp) as max_cv_tstamp,
    min(spend) as spend,
    sum(conversion_total_revenue) as sum_conversion_total_revenue,
    sum(linear_attribution) as attributed_revenue
    
  from channel_prep
  
  {{ dbt_utils.group_by(n=3) }}
  
  union all
  
    select
    'channel' as path_type,
    'position_based' as attribution_type,
    channel as touch_point,
    count(distinct event_id) as n_conversions,
    min(cv_tstamp) as min_cv_tstamp,
    max(cv_tstamp) as max_cv_tstamp,
    min(spend) as spend,
    sum(conversion_total_revenue) as sum_conversion_total_revenue,
    sum(position_based_attribution) as attributed_revenue
    
  from channel_prep
  
  {{ dbt_utils.group_by(n=3) }}
  
union all

  select
    'campaign' as path_type,
    'first_touch' as attribution_type,
    campaign as touch_point,
    count(distinct event_id) as n_conversions,
    min(cv_tstamp) as min_cv_tstamp,
    max(cv_tstamp) as max_cv_tstamp,
    min(spend) as spend,
    sum(conversion_total_revenue) as sum_conversion_total_revenue,
    sum(first_touch_attribution) as attributed_revenue
    
  from campaign_prep
  
  {{ dbt_utils.group_by(n=3) }}
  
  union all
  
    select
    'campaign' as path_type,
    'last_touch' as attribution_type,
    campaign as touch_point,
    count(distinct event_id) as n_conversions,
    min(cv_tstamp) as min_cv_tstamp,
    max(cv_tstamp) as max_cv_tstamp,
    min(spend) as spend,
    sum(conversion_total_revenue) as sum_conversion_total_revenue,
    sum(last_touch_attribution) as attributed_revenue
    
  from campaign_prep
  
  {{ dbt_utils.group_by(n=3) }}
  
  union all
  
    select
    'campaign' as path_type,
    'linear' as attribution_type,
    campaign as touch_point,
    count(distinct event_id) as n_conversions,
    min(cv_tstamp) as min_cv_tstamp,
    max(cv_tstamp) as max_cv_tstamp,
    min(spend) as spend,
    sum(conversion_total_revenue) as sum_conversion_total_revenue,
    sum(linear_attribution) as attributed_revenue
    
  from campaign_prep
  
  {{ dbt_utils.group_by(n=3) }}

  union all
  
    select
    'campaign' as path_type,
    'position_based' as attribution_type,
    campaign as touch_point,
    count(distinct event_id) as n_conversions,
    min(cv_tstamp) as min_cv_tstamp,
    max(cv_tstamp) as max_cv_tstamp,
    min(spend) as spend,
    sum(conversion_total_revenue) as sum_conversion_total_revenue,
    sum(position_based_attribution) as attributed_revenue
    
  from campaign_prep
  
  {{ dbt_utils.group_by(n=3) }}
  
)

select
  path_type,
  attribution_type,
  touch_point,
  sum(n_conversions) as n_conversions,
  min(min_cv_tstamp) as min_cv_tstamp,
  max(max_cv_tstamp) as max_cv_tstamp,
  sum(spend) as spend,
  sum(sum_conversion_total_revenue) as sum_conversion_total_revenue,
  sum(attributed_revenue) as attributed_revenue

  {% if var('snowplow__spend_source')!="{{ source('atomic', 'events') }}" %}
    , sum(attributed_revenue) / nullif(sum(spend), 0) as ROAS
  {% endif %}

from unions

where touch_point is not null

{{ dbt_utils.group_by(n=3) }}
  
{% endmacro %}
