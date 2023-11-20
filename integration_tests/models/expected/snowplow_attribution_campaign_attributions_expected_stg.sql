{#
Copyright (c) 2024-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}



select
  composite_key,
  event_id,
  customer_id,
  cv_tstamp,
  -- due to Databricks
  round(cast(cv_total_revenue as {{ type_numeric() }}), 2) as cv_total_revenue,
  campaign_transformed_path,
  campaign,
  source_index,
  path_length,
  round(cast(first_touch_attribution as {{ type_numeric() }}), 2) as first_touch_attribution,
  round(cast(last_touch_attribution as {{ type_numeric() }}), 2) as last_touch_attribution,
  round(cast(linear_attribution as {{ type_numeric() }}), 2) as linear_attribution,
  round(cast(position_based_attribution as {{ type_numeric() }}), 2) as position_based_attribution

from {{ ref('snowplow_attribution_campaign_attributions_expected') }}
