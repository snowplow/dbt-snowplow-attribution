{#
Copyright (c) 2024-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}


-- page view context is given as json string in csv. Parse json
with prep as (

select
  *,
  parse_json(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0) as contexts_com_snowplowanalytics_snowplow_web_page_1

from {{ ref('snowplow_unified_events') }}
)


select
  *

from prep
