{% macro report_table() %}
    {{ return(adapter.dispatch('report_table', 'snowplow_attribution')()) }}
{% endmacro %}


{% macro default__report_table() %}

with spends as (

    select
      '2022-06-01' as spend_date,
      'Direct' as channel,
      100000 as spend
      
    union all
    
    select
      '2022-06-01',
      'Organic_Search' as channel,
      100000 as spend
      
    union all
    
    select
      '2022-06-01',
      'Video' as channel,
      100000 as spend
    
)

, revenue as (
  
  select
    channel,
    count(distinct event_id) as conversions,
    sum(revenue) as revenue
    
  from spends s
  
  left join {{ target.schema ~ '_derived.snowplow_attribution_channel_attributions'}} c
  
  on c.channel_path = s.channel and s.spend_date < c.cv_tstamp 
  and {{ dbt.dateadd('day', 90, 'spend_date') }} > c.cv_tstamp
  
  group by 1
  
)
  
select 
  r.channel,
  r.conversions,
  r.revenue,
  s.spend,
  coalesce(revenue,0) / nullif(spend, 0) as ROAS

from revenue r
left join spends s
on r.channel = s.channel 
  
{% endmacro %}
