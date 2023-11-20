{#
Copyright (c) 2024-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

select
  path_type,
  attribution_type,
  touch_point,
  in_n_conversion_paths,
  round(cast(attributed_conversions as {{ type_numeric() }}), 2) as attributed_conversions,
  min_cv_tstamp,
  max_cv_tstamp,
  spend,
  -- due to Databricks
  round(cast(sum_cv_total_revenue as {{ type_numeric() }}), 2) as sum_cv_total_revenue,
  round(cast(attributed_revenue as {{ type_numeric() }}), 2) as attributed_revenue,
  round(cast(sum_cv_total_revenue as {{ type_numeric() }}), 10) as roas

from {{ ref('snowplow_attribution_overview_expected') }}
