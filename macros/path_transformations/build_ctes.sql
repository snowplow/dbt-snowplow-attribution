{#
Copyright (c) 2024-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

/* macro is for spark only */
/* Macro to remove complexity from model "transform_paths" for building spark CTEs. */

{% macro build_ctes(path_transform_name, parameter, model_type) %}
  {{ return(adapter.dispatch('build_ctes', 'snowplow_attribution')(path_transform_name, parameter, model_type)) }}
{% endmacro %}

{% macro default__build_ctes(path_transform_name, parameter, model_type) %}
{% endmacro %}

{% macro spark__build_ctes(path_transform_name, parameter, model_type) %}

  select
    customer_id,
    {% if model_type == 'conversions' %}
      cv_id,
      event_id,
      cv_tstamp,
      cv_type,
      cv_path_start_tstamp,
      revenue,
    {% endif %}
    channel_path,
    {% if path_transform_name == 'unique_path' %}
      {{ path_transformation('unique_path', field_alias='channel') }} as channel_transformed_path,
    {% elif path_transform_name == 'frequency_path' %}
      {{ exceptions.raise_compiler_error(
        "Snowplow Error: Frequency path is currently not supported by the model, please remove it from the variable and use this path transformation function in a custom model."
      ) }}

    {% elif path_transform_name == 'first_path' %}
      {{ path_transformation('first_path', field_alias='channel') }} as channel_transformed_path,

    {% elif path_transform_name == 'exposure_path' %}
      {{ path_transformation('exposure_path', field_alias='channel') }} as channel_transformed_path,

    {% elif path_transform_name == 'remove_if_not_all' %}
      {{ path_transformation('remove_if_not_all', parameter, 'channel') }} as channel_transformed_path,

    {% elif path_transform_name == 'remove_if_last_and_not_all' %}
      {{ path_transformation('remove_if_last_and_not_all', parameter, 'channel') }} as channel_transformed_path,
    
    {% else %}
      {%- do exceptions.raise_compiler_error("Snowplow Error: the path transform - '"+path_transform_name+"' - is not supported. Please refer to the Snowplow docs on tagging. Please use one of the following: exposure_path, first_path, frequency_path, remove_if_last_and_not_all, remove_if_not_all, unique_path") %}
    {% endif %}
    
    campaign_path,
    {% if path_transform_name == 'unique_path' %}
      {{ path_transformation('unique_path', field_alias='campaign') }} as campaign_transformed_path

    {% elif path_transform_name == 'frequency_path' %}
      {{ exceptions.raise_compiler_error(
        "Snowplow Error: Frequency path is currently not supported by the model, please remove it from the variable and use this path transformation function in a custom model."
      ) }}

    {% elif path_transform_name == 'first_path' %}
      {{ path_transformation('first_path', field_alias='campaign') }} as campaign_transformed_path

    {% elif path_transform_name == 'exposure_path' %}
      {{ path_transformation('exposure_path', field_alias='campaign') }} as campaign_transformed_path

    {% elif path_transform_name == 'remove_if_not_all' %}
      {{ path_transformation('remove_if_not_all', parameter, 'campaign') }} as campaign_transformed_path

    {% elif path_transform_name == 'remove_if_last_and_not_all' %}
      {{ path_transformation('remove_if_last_and_not_all', parameter, 'campaign') }} as campaign_transformed_path

    {% else %}
      {%- do exceptions.raise_compiler_error("Snowplow Error: the path transform - '"+path_transform_name+"' - is not supported. Please refer to the Snowplow docs on tagging. Please use one of the following: exposure_path, first_path, frequency_path, remove_if_last_and_not_all, remove_if_not_all, unique_path") %}
    {% endif %}

{% endmacro %}
