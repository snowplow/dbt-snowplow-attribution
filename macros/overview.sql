{% macro overview() %}
    {{ return(adapter.dispatch('overview', 'snowplow_attribution')()) }}
{% endmacro %}


{% macro default__overview() %}

with campaign_prep as (
  
  select
    c.event_id,
    c.campaign,
    c.cv_tstamp,
    count(*) as steps,
    sum(c.first_touch_attribution) as first_touch_attribution,
    sum(c.last_touch_attribution) as last_touch_attribution,
    sum(c.linear_attribution) as linear_attribution,
    sum(c.position_based_attribution) as position_based_attribution,
    coalesce(min(c.conversion_total_revenue),0) as conversion_total_revenue,
    
    {% if var('snowplow__spend_source') != 'not defined' %}
      min(s.spend) as spend
    {% else %}
      null as spend
    {% endif %}
    
  from {{ ref('snowplow_attribution_campaign_attributions') }} c
  
  {% if var('snowplow__spend_source') != 'not defined' %}
    left join {{ var('snowplow__spend_source') }} s
    on c.campaign = s.campaign and s.period < cv_tstamp 
    and s.period > {{ dbt.dateadd('day', -90, 'cv_tstamp') }}
  {% endif %}
  
  -- where cv_tstamp >= '2023-01-01' and cv_tstamp < '2023-03-01'
  
  {{ dbt_utils.group_by(n=3) }}
)

, channel_prep as (
  
  select
    c.event_id,
    c.cv_tstamp,
    c.channel,
    count(*) as steps,
    sum(c.first_touch_attribution) as first_touch_attribution,
    sum(c.last_touch_attribution) as last_touch_attribution,
    sum(c.linear_attribution) as linear_attribution,
    sum(c.position_based_attribution) as position_based_attribution,
    coalesce(min(c.conversion_total_revenue),0) as conversion_total_revenue,
  
  {% if var('snowplow__spend_source') != 'not defined' %}
    min(s.spend) as spend
  {% else %}
    null as spend
  {% endif %}
  
  from {{ ref('snowplow_attribution_channel_attributions') }} c
  
  {% if var('snowplow__spend_source') != 'not defined' %}
    left join {{ var('snowplow__spend_source') }} s
    on c.channel = s.channel and s.period < cv_tstamp 
    and s.period > {{ dbt.dateadd('day', -90, 'cv_tstamp') }}
  {% endif %}
  
  -- where cv_tstamp >= '2023-01-01' and cv_tstamp < '2023-03-01'

  {{ dbt_utils.group_by(n=3) }}
)

, unions as (
  
  select
    'channel' as path_type,
    'first_touch' as attribution_type,
    channel as touch_point,
    sum(case when first_touch_attribution is not null then steps end) as steps,
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
    sum(case when last_touch_attribution is not null then steps end) as steps,
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
    sum(case when linear_attribution is not null then steps end) as steps,
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
    sum(case when position_based_attribution is not null then steps end) as steps,
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
    sum(case when first_touch_attribution is not null then steps end) as steps,
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
    sum(case when last_touch_attribution is not null then steps end) as steps,
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
    sum(case when linear_attribution is not null then steps end) as steps,
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
    sum(case when position_based_attribution is not null then steps end) as steps,
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
  *

  {% if var('snowplow__spend_source') != 'not defined' %}
    , sum(attributed_revenue) / nullif(sum(spend), 0) as ROAS
  {% endif %}

from unions

where touch_point is not null

{{ dbt_utils.group_by(n=10) }}
  
{% endmacro %}
