{#
Copyright (c) 2024-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% macro get_path_lookback_start(earliest_cv_tstamp, path_lookback_days) %}
  {{ return(adapter.dispatch('get_path_lookback_start', 'snowplow_attribution')(earliest_cv_tstamp, path_lookback_days)) }}
{% endmacro %}

{% macro default__get_path_lookback_start(earliest_cv_tstamp, path_lookback_days) %}
  {{ return(snowplow_utils.timestamp_add('day', -path_lookback_days, dbt.date_trunc('day', earliest_cv_tstamp))) }}
{% endmacro %}

{% macro spark__get_path_lookback_start(earliest_cv_tstamp, path_lookback_days) %}
  {# Spark timestamp_add uses fixed 24-hour days; dateadd preserves calendar midnight across DST. #}
  {{ return(dbt.dateadd('day', -path_lookback_days, dbt.date_trunc('day', earliest_cv_tstamp))) }}
{% endmacro %}

{% macro databricks__get_path_lookback_start(earliest_cv_tstamp, path_lookback_days) %}
  {# Databricks inherits Spark dispatch, but its existing timestampadd already uses calendar days. #}
  {{ return(snowplow_attribution.default__get_path_lookback_start(earliest_cv_tstamp, path_lookback_days)) }}
{% endmacro %}
