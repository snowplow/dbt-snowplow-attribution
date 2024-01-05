{% macro report_table() %}
    {{ return(adapter.dispatch('report_table', 'snowplow_attribution')()) }}
{% endmacro %}


{% macro default__report_table() %}

with prep as (
  
  select
    'channel' as path_type,
    ch.channel_path as path,
    coalesce(sum(ch.revenue),0) as revenue,
    coalesce(sum(s.spend),0) as spend
    
  from {{ ref('snowplow_attribution_channel_attributions') }} ch
  
  left join {{ var('snowplow__spend_source') }} s
  on ch.channel_path = s.path and s.period < ch.cv_tstamp 
  and s.period > {{ dbt.dateadd('day', -90, 'ch.cv_tstamp') }}
  
  group by 1,2
  
  union all
  
  select
    'campaign' as path_type,
    camp.campaign_path as path,
    coalesce(sum(camp.revenue),0) as revenue,
    coalesce(sum(s.spend),0) as spend
    
  from {{ ref('snowplow_attribution_campaign_attributions') }} camp
  
  left join {{ var('snowplow__spend_source') }} s
  on camp.campaign_path = s.path and s.period < camp.cv_tstamp 
  and s.period > {{ dbt.dateadd('day', -90, 'camp.cv_tstamp') }}
  
  group by 1,2
  
)

select
  path_type,
  path,
  spend,
  revenue,
  coalesce(revenue,0) / nullif(spend, 0) as ROAS

from prep

{% endmacro %}
