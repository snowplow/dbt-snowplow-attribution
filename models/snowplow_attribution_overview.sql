{#
Copyright (c) 2024-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}


{{
  config(
    enabled=var('snowplow__enable_attribution_overview'),
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    materialized= 'table' if target.type in ['spark'] else 'view'
  )
}}


{{ attribution_overview() }}
