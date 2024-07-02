/*  Macro to make sure the user does not execute the package with missing conversion or path source data. */

{% macro source_checks() %}
  {{ return(adapter.dispatch('source_checks', 'snowplow_attribution')()) }}
{% endmacro %}

{% macro default__source_checks() %}

  {% set conversions_source_relation = snowplow_attribution.get_relation_from_string(var('snowplow__conversions_source', '')) or source('derived', 'snowplow_unified_conversions') %}
  {% set conversion_path_source_relation = snowplow_attribution.get_relation_from_string(var('snowplow__conversion_path_source', '')) or source('derived', 'snowplow_unified_views') %}
  {%- set __, last_cv_tstamp = snowplow_utils.return_limits_from_model(conversions_source_relation,'cv_tstamp','cv_tstamp') %}
  {%- set __, last_path_tstamp = snowplow_utils.return_limits_from_model(conversion_path_source_relation,'start_tstamp','start_tstamp') %}
  {%- set __, last_processed_cv_tstamp = snowplow_utils.return_limits_from_model(this,'cv_path_start_tstamp','cv_path_start_tstamp') %}

  {% if is_incremental() %}
    {% if last_cv_tstamp < last_processed_cv_tstamp %}
    {{ exceptions.raise_compiler_error(
"Snowplow Error: The timestamp of the last conversion event in the conversion source ("~ var('snowplow__conversions_source',source('derived', 'snowplow_unified_conversions')) ~ "):" ~ last_cv_tstamp ~ 
    " is lower than the timestamp of the last processed conversion " ~ last_processed_cv_tstamp ~ 
    "within the model "~ this ~ ". Please make sure you have updated downstream sources before proceeding."
    ) }}
    {% endif %}
  {% endif %}
  
  {% set query %}
    with prep as (
      select {{ last_path_tstamp }} as last_path_tstamp, {{ last_cv_tstamp }} as last_cv_tstamp
    )
      select {{ snowplow_utils.timestamp_add('day', var('snowplow__path_lookback_days'), 'last_path_tstamp' ) }}  < last_cv_tstamp as is_below_limit
      from prep
  {% endset %}

  {% set result = run_query(query) %}

  {% if execute %}
    {% set is_below_limit = result[0][0] %}
    {% if is_below_limit == True %}
      {{ exceptions.raise_compiler_error("Snowplow Error: The timestamp of the last visit in the path source: " ~ last_path_tstamp ~ 
      " plus the snowplow__path_lookback_days " ~ var('snowplow__path_lookback_days') ~ " is lower than the timestamp of the last conversion in the conversion source" ~ last_processed_cv_tstamp ~ 
      " Please make sure you have updated downstream sources before proceeding."
      ) }}
    {% endif %}
  {% endif %}

{% endmacro %}
