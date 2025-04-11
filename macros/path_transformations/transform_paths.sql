{#
Copyright (c) 2024-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}


/* Macro to remove complexity from models paths_to_conversion / paths_to_non_conversion. */

{% macro transform_paths(model_type) %}
  {{ return(adapter.dispatch('transform_paths', 'snowplow_attribution')(model_type)) }}
{% endmacro %}

{% macro default__transform_paths(model_type) %}

  {% set allowed_path_transforms = ['exposure_path', 'first_path', 'remove_if_last_and_not_all', 'remove_if_not_all', 'unique_path'] %}

  , path_transforms as (

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
      campaign_path,

    {% if var('snowplow__path_transforms').items() %}
      -- 1. do transformations on channel_transformed_path:
      -- reverse transormation due to nested functions, items to be processed from left to right
      {% for path_transform_name, transform_param in var('snowplow__path_transforms').items()|reverse %}
        {% if path_transform_name not in allowed_path_transforms %}
          {%- do exceptions.raise_compiler_error("Snowplow Error: the path transform - '"+path_transform_name+"' - is not supported. Please refer to the Snowplow docs on tagging. Please use one of the following: exposure_path, first_path, remove_if_last_and_not_all, remove_if_not_all, unique_path") %}
        {% endif %}
        {% if transform_param %}
          {% for _ in range(transform_param|length) %}
            {{target.schema}}.{{path_transform_name}}(
          {% endfor %}
        {% else %}
          {{target.schema}}.{{path_transform_name}}(
        {% endif %}
      {% endfor %}

      channel_transformed_path

      -- no reverse needed due to nested nature of function calls
      {% for _, transform_param in var('snowplow__path_transforms').items() %}
        {% if transform_param %}
          {% for parameter in transform_param %}
            ,'{{parameter}}')
          {% endfor %}
        {% else %}
            )
        {% endif %}
      {% endfor %}

      as channel_transformed_path, 

    {% else %}
     channel_transformed_path, 
    {% endif %}
    

    {% if var('snowplow__path_transforms').items() %}
    -- 2. do transformations on campaign_transformed_path:
      -- reverse transormation due to nested functions, items to be processed from left to right

      {% for path_transform_name, transform_param in var('snowplow__path_transforms').items()|reverse %}
        {% if path_transform_name not in allowed_path_transforms %}
          {%- do exceptions.raise_compiler_error("Snowplow Error: the path transform - '"+path_transform_name+"' - is not supported. Please refer to the Snowplow docs on tagging. Please use one of the following: exposure_path, first_path, remove_if_last_and_not_all, remove_if_not_all, unique_path") %}
        {% endif %}
        {% if transform_param %}
          {% for _ in range(transform_param|length) %}
            {{target.schema}}.{{path_transform_name}}(
          {% endfor %}
        {% else %}
          {{target.schema}}.{{path_transform_name}}(
        {% endif %}
      {% endfor %}

      campaign_transformed_path

      -- no reverse needed due to nested nature of function calls
      {% for _, transform_param in var('snowplow__path_transforms').items() %}
        {% if transform_param %}
          {% for parameter in transform_param %}
            ,'{{parameter}}')
          {% endfor %}
        {% else %}
            )
        {% endif %}
      {% endfor %}

      as campaign_transformed_path

    {% else %}
     campaign_transformed_path
    {% endif %}
    
  from trim_long_path_cte

  )

{% endmacro %}


{% macro spark__transform_paths(model_type) %}

  -- set namespace to define as global variables for the loop to work
  {% set loop_count = namespace(value=1) %}
  {% set total_transformations = namespace(count=0) %} 
  {% set previous_cte = namespace(value=null) %}
  

  -- unlike for adapters using UDFS, reverse transormation is not needed as ctes will process items their params in order
  {% for path_transform_name, transform_param in var('snowplow__path_transforms').items() %}

    {%- if loop_count.value == 1 %}
      {% set previous_cte.value = "trim_long_path" %}
    {% else %}
      {% set previous_cte.value = loop_count.value-1 %}
    {% endif %}
    
    {% if path_transform_name in ['remove_if_not_all', 'remove_if_last_and_not_all'] and transform_param %}
    
      {% for parameter in transform_param %}
        
        {% set total_transformations.count = total_transformations.count+1 %}

        , transformation_{{ loop_count.value|string }} as (
          
            {{ build_ctes(path_transform_name, parameter, model_type) }}

        {%- if loop_count.value == 1 %}
         from trim_long_path_cte
         )
        {% else %}
        -- build cte names dynamically based on loop count / previous_cte for the loop to work regardless of array items
        from transformation_{{ previous_cte.value|string }}
        )
        {% endif %}
        {% set loop_count.value = loop_count.value + 1 %}
        {% set previous_cte.value = loop_count.value-1 %}

      {% endfor %}

    {% else %}
    
      {% set total_transformations.count = total_transformations.count+1 %}
      
      , transformation_{{ loop_count.value|string }} as (
          
          {{ build_ctes(path_transform_name, transform_param, model_type) }}

        {%- if loop_count.value == 1 %}
        from trim_long_path_cte
        )
        {% else %}
        -- build cte names dynamically based on loop count / previous_cte for the loop to work regardless of array items
        from transformation_{{ previous_cte.value|string }}
        )
        {% endif %}
        {% set loop_count.value = loop_count.value + 1 %}
      
    {% endif %}
  {% endfor %}

  , path_transforms as (

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
      channel_transformed_path,
      campaign_path,
      campaign_transformed_path

  -- the last cte will always equal to the total transformations unless there is no item there
  {% if total_transformations.count > 0 %}
    from transformation_{{ total_transformations.count }}

  {% else %}
    from trim_long_path_cte
  {% endif %}
  )

{% endmacro %}
