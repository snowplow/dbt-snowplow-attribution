{#
Copyright (c) 2024-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}



{% macro validate_path_transforms() %}
  {{ return(adapter.dispatch('validate_path_transforms', 'snowplow_attribution')()) }}
{% endmacro %}

{% macro default__validate_path_transforms() %}

  {% set allowed_path_transforms = ['exposure_path', 'first_path', 'remove_if_last_and_not_all', 'remove_if_not_all', 'unique_path'] %}

  {% for path_transform_name, transform_param in var('snowplow__path_transforms').items() %}

    {% if path_transform_name not in allowed_path_transforms %}
      {%- do exceptions.raise_compiler_error("Snowplow Error: the path transform - '"+path_transform_name+"' - is not supported. Please refer to the Snowplow docs on tagging. Please use one of the following: exposure_path, first_path, remove_if_last_and_not_all, remove_if_not_all, unique_path") %}
    {% endif %}

    {% if path_transform_name in ['remove_if_not_all', 'remove_if_last_and_not_all'] %}

      -- raise exception if transform_param is not of data type list for 'remove_if_not_all' and 'remove_if_last_and_not_all' path transforms
      {% if not (transform_param is iterable and transform_param is sequence and transform_param is not mapping and transform_param is not string) %}
        {%- do exceptions.raise_compiler_error("Snowplow Error: the dict value data type for both 'remove_if_not_all' and 'remove_if_last_and_not_all' path transforms needs to be a list. The provided - '"+transform_param+"' - is invalid.") %}
      {% endif %}

      {% if transform_param == [] %}
        {%- do exceptions.raise_compiler_error("Snowplow Error: An empty list is provided for transform - '"+path_transform_name+"' - Please provide at least one list member.") %}
      {% endif %}

    {% endif %}
  {% endfor %}


{% endmacro %}
