{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}


-- test dataset includes page_view_id as part of events table.
-- RS and PG events tables are federated so split out page_view_id into its own table

with prep as (
select
  event_id as root_id,
  collector_tstamp as root_tstamp,
  split_part(split_part(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0,'[{"id":"', 2), '"}]', 1) as id -- test dataset uses json format. Extract.

from {{ ref('snowplow_attribution_events') }}
)

select
  root_id,
  root_tstamp,
  'page_view_context' as schema_name,
  case when id = 'null' or id = '' then null else id end as id

from prep
