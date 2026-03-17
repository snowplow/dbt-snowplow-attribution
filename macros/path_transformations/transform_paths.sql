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

  {{ validate_path_transforms() }}

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
      -- reverse transformation due to nested functions, items to be processed from left to right
      {% for path_transform_name, transform_param in var('snowplow__path_transforms').items()|reverse %}

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
      -- reverse transformation due to nested functions, items to be processed from left to right

      {% for path_transform_name, transform_param in var('snowplow__path_transforms').items()|reverse %}

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

{% macro redshift__transform_paths(model_type) %}

  {{ validate_path_transforms() }}

  {% set partition_by = 'cv_id' if model_type == 'conversions' else 'customer_id' %}

  , analytic_windows as (
    select
      {% if model_type == 'conversions' %}
        cv_id,
        event_id,
        customer_id,
        cv_tstamp,
        cv_type,
        cv_path_start_tstamp,
        revenue,
      {% else %}
        customer_id,
      {% endif %}
      channel,
      campaign,
      visit_start_tstamp,
      row_number() over (partition by {{ partition_by }} order by visit_start_tstamp desc) as reverse_step,
      lag(channel)  over (partition by {{ partition_by }} order by visit_start_tstamp) as prev_channel,
      lag(campaign) over (partition by {{ partition_by }} order by visit_start_tstamp) as prev_campaign,
      row_number() over (partition by {{ partition_by }}, channel  order by visit_start_tstamp) as channel_occ,
      row_number() over (partition by {{ partition_by }}, campaign order by visit_start_tstamp) as campaign_occ
    from vertical_path_base
  )

  , base_windows as (
    select
      *,
      {% for path_transform_name, transform_param in var('snowplow__path_transforms').items() %}
        {% if path_transform_name == 'remove_if_not_all' %}
          {% for parameter in transform_param %}
            sum(case when channel  != '{{ parameter }}' then 1 else 0 end) over (partition by {{ partition_by }}) as channel_non_param_count,
            sum(case when campaign != '{{ parameter }}' then 1 else 0 end) over (partition by {{ partition_by }}) as campaign_non_param_count,
          {% endfor %}
        {% elif path_transform_name == 'remove_if_last_and_not_all' %}
          {% for parameter in transform_param %}
            max(case when channel  != '{{ parameter }}' then visit_start_tstamp end) over (partition by {{ partition_by }}) as last_non_param_channel_tstamp,
            max(case when campaign != '{{ parameter }}' then visit_start_tstamp end) over (partition by {{ partition_by }}) as last_non_param_campaign_tstamp,
          {% endfor %}
        {% endif %}
      {% endfor %}
      1 as _dummy
    from analytic_windows
  )

  -- only trim_long_path filtering here, no transform filtering
  , trim_long_path_cte as (
    select *
    from base_windows
    where 1=1
    {% if var('snowplow__path_lookback_steps') and var('snowplow__path_lookback_steps') > 0 %}
      and reverse_step <= {{ var('snowplow__path_lookback_steps') }}
    {% endif %}
  )

  -- Note: Redshift listagg silently drops empty strings unlike other adapters.
  -- Empty string channel values will be excluded from path strings on Redshift.
  -- This is a known limitation of Redshift's listagg function.
  , path_transforms as (
    select
      {% if model_type == 'conversions' %}
        cv_id,
        event_id,
        customer_id,
        cv_tstamp,
        cv_type,
        cv_path_start_tstamp,
        revenue,
      {% else %}
        customer_id,
      {% endif %}
      listagg(channel,  ' > ') within group (order by visit_start_tstamp) as channel_path,
      listagg(campaign, ' > ') within group (order by visit_start_tstamp) as campaign_path,

      {% for path_transform_name, transform_param in var('snowplow__path_transforms').items() %}

        {% if path_transform_name == 'exposure_path' %}
          listagg(case when prev_channel is null or channel != prev_channel then channel end, ' > ')
            within group (order by visit_start_tstamp) as channel_transformed_path,
          listagg(case when prev_campaign is null or campaign != prev_campaign then campaign end, ' > ')
            within group (order by visit_start_tstamp) as campaign_transformed_path

        {% elif path_transform_name == 'unique_path' %}
          listagg(channel, ' > ') within group (order by visit_start_tstamp) as channel_transformed_path,
          listagg(campaign, ' > ') within group (order by visit_start_tstamp) as campaign_transformed_path

        {% elif path_transform_name == 'first_path' %}
          listagg(case when channel_occ = 1 then channel end, ' > ')
            within group (order by visit_start_tstamp) as channel_transformed_path,
          listagg(case when campaign_occ = 1 then campaign end, ' > ')
            within group (order by visit_start_tstamp) as campaign_transformed_path

        {% elif path_transform_name == 'remove_if_not_all' %}
          {% for parameter in transform_param %}
            listagg(case when channel != '{{ parameter }}' or channel_non_param_count = 0 then channel end, ' > ')
              within group (order by visit_start_tstamp) as channel_transformed_path,
            listagg(case when campaign != '{{ parameter }}' or campaign_non_param_count = 0 then campaign end, ' > ')
              within group (order by visit_start_tstamp) as campaign_transformed_path
          {% endfor %}

        {% elif path_transform_name == 'remove_if_last_and_not_all' %}
          {% for parameter in transform_param %}
            listagg(case when channel != '{{ parameter }}' or last_non_param_channel_tstamp is null or visit_start_tstamp <= last_non_param_channel_tstamp then channel end, ' > ')
              within group (order by visit_start_tstamp) as channel_transformed_path,
            listagg(case when campaign != '{{ parameter }}' or last_non_param_campaign_tstamp is null or visit_start_tstamp <= last_non_param_campaign_tstamp then campaign end, ' > ')
              within group (order by visit_start_tstamp) as campaign_transformed_path
          {% endfor %}

        {% else %}
          {%- do exceptions.raise_compiler_error(
            "Snowplow Error: the path transform - '" ~ path_transform_name ~ "' - is not supported. Please use one of: exposure_path, first_path, remove_if_last_and_not_all, remove_if_not_all, unique_path"
          ) %}
        {% endif %}

      {% endfor %}

      {% if var('snowplow__path_transforms') | length == 0 %}
        listagg(channel, ' > ') within group (order by visit_start_tstamp) as channel_transformed_path,
        listagg(campaign, ' > ') within group (order by visit_start_tstamp) as campaign_transformed_path
      {% endif %}

    from trim_long_path_cte
    {% if model_type == 'conversions' %}
      group by 1,2,3,4,5,6,7
    {% else %}
      group by 1
    {% endif %}
  )

{% endmacro %}

{% macro spark__transform_paths(model_type) %}

  {{ validate_path_transforms() }}

  -- set namespace to define as global variables for the loop to work
  {% set loop_count = namespace(value=1) %}
  {% set total_transformations = namespace(count=0) %} 
  {% set previous_cte = namespace(value=null) %}
  

  -- unlike for adapters using UDFS, reverse transformation is not needed as ctes will process items their params in order
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
          
            {{ build_sql(path_transform_name, parameter, model_type) }}

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
          
          {{ build_sql(path_transform_name, transform_param, model_type) }}

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
