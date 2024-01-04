{#
Copyright (c) 2024-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}


{{
  config(
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

select
  'channel' as path_type,
  channel_path as path,
  count(*) as number_of_sessions

from {{ ref('snowplow_attribution_channel_attributions') }}

group by 1,2

union all

select
  'campaign' as path_type,
  campaign_path as path,
  count(*) as number_of_sessions

from {{ ref('snowplow_attribution_campaign_attributions') }}

group by 1,2

order by 1,2,3
