{#
Copyright (c) 2024-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

with spend_with_unique_keys as (

    select row_number() over(order by spend_tstamp) as spend_id, *
    from {{ ref('test_spend_data') }}
)

-- we need to dedupe as the join does the filtering, we can't group them upfront
, campaign_spend as (

    select s.campaign, s.spend, row_number() over (partition by s.spend_id order by s.spend_tstamp) as row_num
    from spend_with_unique_keys s
    inner join {{ ref('snowplow_attribution_campaign_attributions') }} c
    on c.campaign = s.campaign and s.spend_tstamp < cv_tstamp 
    and s.spend_tstamp > {{ snowplow_utils.timestamp_add('day', -90, 'cv_tstamp') }}
    where s.campaign is not null
    
)

, channel_spend as (
  
    select s.channel, s.spend, row_number() over (partition by s.spend_id order by s.spend_tstamp) as row_num
    from spend_with_unique_keys s
    inner join {{ ref('snowplow_attribution_channel_attributions') }} c
    on c.channel = s.channel and s.spend_tstamp < cv_tstamp
    and s.spend_tstamp > {{ snowplow_utils.timestamp_add('day', -90, 'cv_tstamp') }}
    where s.channel is not null
  
)

-- grouping spend to avoid duplicates in later join
, campaign_spend_grouped as (
  
    select campaign as path, sum(spend) as spend
    from campaign_spend
    where row_num = 1
    group by 1
    
)

, channel_spend_grouped as (
  
    select channel as path, sum(spend) as spend
    from channel_spend
    where row_num = 1
    group by 1
    
)

select * from channel_spend_grouped
union all
select * from campaign_spend_grouped

