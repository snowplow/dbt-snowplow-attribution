{#
Copyright (c) 2024-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}


select
  event_id,
  customer_id,
  cv_tstamp,
  cv_path_start_tstamp,
  -- due to Databricks
  round(cast(revenue as {{ type_numeric() }}), 2) as revenue,
  channel_path,
  channel_transformed_path,
  campaign_path,
  campaign_transformed_path
  
from {{ ref('snowplow_attribution_paths_to_conversion') }}
