{#
Copyright (c) 2024-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

with spend as (

    select
      cast('2023-06-01' as {{dbt.type_timestamp()}}) as spend_tstamp,
      'Direct' as channel,
      'Campaign 1' as campaign,
      10 as spend
      
    union all
    
    select
      cast('2023-06-01' as {{dbt.type_timestamp()}}),
      'Direct' as channel,
      'Campaign 1' as campaign,
      10 as spend
      
    union all
    
    select
      cast('2023-06-01' as {{dbt.type_timestamp()}}),
      null as channel,
      'Campaign 1' as campaign,
      100 as spend
      
    union all
    
    select
      cast('2023-06-01' as {{dbt.type_timestamp()}}),
      'Direct' as channel,
      null as campaign,
      1 as spend

    union all
    
    select
      cast('2023-06-01' as {{dbt.type_timestamp()}}),
      null as channel,
      null as campaign,
      10 as spend
    
)

select * from spend
