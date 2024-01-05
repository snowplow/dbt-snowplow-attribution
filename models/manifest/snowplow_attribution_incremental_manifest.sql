{#
Copyright (c) 2024-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{{
  config(
    materialized='incremental',
    full_refresh=snowplow_attribution.allow_refresh(),
    on_schema_change='append_new_columns',
    unique_key='processed_at',
    upsert_date_key='processed_at',
    sort='last_processed_cv_tstamp',
    dist='processed_at',
    partition_by = snowplow_utils.get_value_by_target_type(bigquery_val = {
      "field": "cv_tstamp",
      "data_type": "last_processed_cv_tstamp"
    }, databricks_val='last_processed_cv_tstamp'),
    tags=["derived"],
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    tblproperties={
      'delta.autoOptimize.optimizeWrite' : 'true',
      'delta.autoOptimize.autoCompact' : 'true'
    },
    snowplow_optimize = true
  )
}}

with prep as (
  
  select
    'paths_to_conversion' as model,
    {{ snowplow_utils.current_timestamp_in_utc() }} as processed_at,
    {{ var("snowplow__conversion_hosts") }} as conversion_hosts,
    
    {% for path_transform_name, _ in var('snowplow__path_transforms').items() %}
      '{{ path_transform_name }}'
      {%- if not loop.last %}|| ', '||
      {% else %} as path_transforms,
      {% endif %}
    {% endfor %}

    {{ var("snowplow__path_lookback_steps") }} as path_lookback_steps,
    {{ var("snowplow__path_lookback_days") }} as path_lookback_days,
    {{ var("snowplow__consider_intrasession_channels") }} as consider_intrasession_channels,
    {{ var("snowplow__channels_to_exclude") }} as channels_to_exclude,
    {{ var("snowplow__channels_to_include") }} as channels_to_include,
    max(cv_tstamp) as last_processed_cv_tstamp
    
  from {{ ref('snowplow_attribution_paths_to_conversion') }}

  {{ dbt_utils.group_by(n=9) }}
  
)

select * from prep
