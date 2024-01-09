{#
Copyright (c) 2024-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

with spends as (

    select
      '2022-06-01' as period,
      'Direct' as path,
      100000 as spend
      
    union all
    
    select
      '2022-06-01',
      'Organic_Search' as path,
      100000 as spend
      
    union all
    
    select
      '2022-06-01',
      'Video' as path,
      100000 as spend
    
)

select * from spends