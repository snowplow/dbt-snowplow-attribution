{% macro overview() %}
    {{ return(adapter.dispatch('overview', 'snowplow_attribution')()) }}
{% endmacro %}


{% macro default__overview() %}

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
    on c.channel = s.channel and s.period < cv_tstamp 
    and s.period > {{ dbt.dateadd('day', -90, 'cv_tstamp') }}
  {% endif %}
  
  -- where cv_tstamp >= '2023-01-01' and cv_tstamp < '2023-03-01'

  {{ dbt_utils.group_by(n=3) }}
)

, unions as (
  
  {% set attribution_list = ['first_touch', 'last_touch', 'linear', 'position_based'] %}
  
  {% for attribution in attribution_list %}
    select
      'channel' as path_type,
      '{{ attribution }}' as attribution_type,
      channel as touch_point,
      sum(case when {{ attribution }}_attribution is not null then steps end) as steps,
      count(distinct event_id) as n_conversions,
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
      sum(case when {{ attribution }}_attribution is not null then steps end) as steps,
      count(distinct event_id) as n_conversions,
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
