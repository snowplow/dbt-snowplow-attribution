{#
Copyright (c) 2024-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}


{{
  config(
    enabled=var('snowplow__spend_source')!="{{ source('atomic', 'events') }}"
  )
}}

select *

from {{ ref('snowplow_attribution_roas_expected') }}
where attribution_type = '{{ var("snowplow__attribution_model_for_snowpark", "NULL") }}'