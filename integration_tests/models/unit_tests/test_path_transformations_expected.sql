{#
Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

with expected_result as (

-- test remove_if_last_and_not_all (if not all the same)
  select
    'Example > Video > Direct > Direct' as raw_array,
    'Direct' as trim_long_path,
    'Direct > Direct' as trim_long_path2,
    'Example > Video > Direct > Direct' as unique_path,
    'Example > Video > Direct' as exposure_path,
    'Example > Video > Direct' as first_path,
    'Example > Video' as remove_if_not_all,
    'Example > Video' as remove_if_last_and_not_all

  union all

-- test remove_if_last_and_not_all (if all the same)
  select
    'Direct > Direct' as raw_array,
    'Direct' as trim_long_path,
    'Direct > Direct' as trim_long_path2,
    'Direct > Direct' as unique_path,
    'Direct' as exposure_path,
    'Direct' as first_path,
    'Direct > Direct' as remove_if_not_all,
    'Direct > Direct' as remove_if_last_and_not_all

  union all

-- test frequency_path with a long and varied > repeated pattern
  select
    'a > a > a > Direct > a > Direct > Direct' as raw_array,
    'Direct' as trim_long_path,
    'Direct > Direct' as trim_long_path2,
    'a > a > a > Direct > a > Direct > Direct' as unique_path,
    'a > Direct > a > Direct' as exposure_path,
    'a > Direct' as first_path,
    'a > a > a > a' as remove_if_not_all,
    'a > a > a > Direct > a' as remove_if_last_and_not_all

  union all

-- test one member scenario
  select
    'Direct' as raw_array,
    'Direct' as trim_long_path,
    'Direct' as trim_long_path2,
    'Direct' as unique_path,
    'Direct' as exposure_path,
    'Direct' as first_path,
    'Direct' as remove_if_not_all,
    'Direct' as remove_if_last_and_not_all

  union all

-- test empty string scenario
  select
    '' as raw_array,
    '' as trim_long_path,
    '' as trim_long_path2,
    '' as unique_path,
    '' as exposure_path,
    '' as first_path,
    '' as remove_if_not_all,
    '' as remove_if_last_and_not_all

  union all

-- test all different scenario
  select
    'Example > Video > Direct' as raw_array,
    'Direct' as trim_long_path,
    'Video > Direct' as trim_long_path2,
    'Example > Video > Direct' as unique_path,
    'Example > Video > Direct' as exposure_path,
    'Example > Video > Direct' as first_path,
    'Example > Video' as remove_if_not_all,
    'Example > Video' as remove_if_last_and_not_all

  union all

-- test mixed in with emtpy strings scenario
  select
    'Example > Video > ' as raw_array,
    '' as trim_long_path,
    'Video > ' as trim_long_path2,
    'Example > Video > ' as unique_path,
    'Example > Video > ' as exposure_path,
    'Example > Video > ' as first_path,
    'Example > Video > ' as remove_if_not_all,
    'Example > Video > ' as remove_if_last_and_not_all

)

{% if target.type == 'redshift' %}

select * from expected_result

{% else %}

 , arrays as (

  select
    {{ snowplow_utils.get_split_to_array('raw_array', 'e', ' > ') }} as raw_array,
    {{ snowplow_utils.get_split_to_array('trim_long_path', 'e', ' > ') }} as trim_long_path,
    {{ snowplow_utils.get_split_to_array('trim_long_path2', 'e', ' > ') }} as trim_long_path2,
    {{ snowplow_utils.get_split_to_array('unique_path', 'e', ' > ') }} as unique_path,
    {{ snowplow_utils.get_split_to_array('exposure_path', 'e', ' > ') }} as exposure_path,
    {{ snowplow_utils.get_split_to_array('first_path', 'e', ' > ') }} as first_path,
    {{ snowplow_utils.get_split_to_array('remove_if_not_all', 'e', ' > ') }} as remove_if_not_all,
    {{ snowplow_utils.get_split_to_array('remove_if_last_and_not_all', 'e', ' > ') }} as remove_if_last_and_not_all

  from expected_result e
)

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
