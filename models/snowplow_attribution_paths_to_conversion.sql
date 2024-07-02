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
    pre_hook=[
      "{{ source_checks() }}",
      "SET hive.exec.dynamic.partition.mode=nonstrict"
    ],
    unique_key='cv_id',
    upsert_date_key='cv_tstamp',
    sort='cv_tstamp',
    dist='cv_id',
    partition_by = snowplow_utils.get_value_by_target_type(bigquery_val = {
      "field": "cv_tstamp",
      "data_type": "timestamp"
    }, databricks_val='cv_tstamp_date'),
    cluster_by=snowplow_utils.get_value_by_target_type(bigquery_val=["cv_id","customer_id"], snowflake_val=["to_date(cv_tstamp)"]),
    tags=["derived"],
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    tblproperties={
      'delta.autoOptimize.optimizeWrite' : 'true',
      'delta.autoOptimize.autoCompact' : 'true'
    },
    snowplow_optimize = true
  )
}}

{{ paths_to_conversion() }}
{% if target.type in ['databricks', 'spark'] %}
order by cv_tstamp_date asc
{% endif -%}
