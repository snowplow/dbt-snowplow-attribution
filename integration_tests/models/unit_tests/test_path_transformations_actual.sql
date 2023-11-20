{#
Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

with raw_data as (

    select
       transformed_path
       {% if target.type in ['databricks', 'spark'] %}
         , transformed_path as test_transformed_path
       {% else %}
       {% endif %}
    from {{ ref('test_path_transformations_data') }}

  )

  , arrays as (
    select
      transformed_path as raw_array,
      {{ snowplow_attribution.trim_long_path('transformed_path', 1) }} as trim_long_path,
      {{ snowplow_attribution.trim_long_path('transformed_path', 2) }} as trim_long_path2,
      {{ snowplow_attribution.path_transformation('unique_path', '', 'test') }} as unique_path,
      {{ snowplow_attribution.path_transformation('exposure_path', '', 'test') }} as exposure_path,
      {{ snowplow_attribution.path_transformation('first_path', '', 'test') }} as first_path,
      {{ snowplow_attribution.path_transformation('remove_if_not_all', 'Direct', 'test') }} as remove_if_not_all,
      {{ snowplow_attribution.path_transformation('remove_if_last_and_not_all', 'Direct', 'test') }} as remove_if_last_and_not_all

  from raw_data d
  )

  {% if target.type == 'redshift' %}


  select
     *

  from arrays a


{% else %}

  select
    {{ snowplow_utils.get_array_to_string('raw_array', 'a', delimiter=' > ') }} as raw_array,
    {{ snowplow_utils.get_array_to_string('trim_long_path', 'a', delimiter=' > ') }} as trim_long_path,
    {{ snowplow_utils.get_array_to_string('trim_long_path2', 'a', delimiter=' > ') }} as trim_long_path2,
    {{ snowplow_utils.get_array_to_string('unique_path', 'a', delimiter=' > ') }} as unique_path,
    {{ snowplow_utils.get_array_to_string('exposure_path', 'a', delimiter=' > ') }} as exposure_path,
    {{ snowplow_utils.get_array_to_string('first_path', 'a', delimiter=' > ') }} as first_path,
    {{ snowplow_utils.get_array_to_string('remove_if_not_all', 'a', delimiter=' > ') }} as remove_if_not_all,
    {{ snowplow_utils.get_array_to_string('remove_if_last_and_not_all', 'a', delimiter=' > ') }} as remove_if_last_and_not_all

  from arrays a

{% endif %}
