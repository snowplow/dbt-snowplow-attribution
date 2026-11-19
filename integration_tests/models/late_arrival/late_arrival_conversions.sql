{#
Copyright (c) 2024-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{# Conversions source for the late-arrival scenario: rebuilt with snowplow__late_arrival_run = 1, then 2,
   to simulate conversions landing in the source after the attribution model has already moved past them. #}

select
  cv_id,
  event_id,
  user_identifier,
  cv_tstamp,
  cv_type,
  cv_value
  {% if target.type in ['databricks', 'spark'] -%}
  , date(cv_tstamp) as cv_tstamp_date
  {%- endif %}

from {{ ref('late_arrival_conversions_seed') }}

where arrives_in_run <= {{ var('snowplow__late_arrival_run', 1) }}
