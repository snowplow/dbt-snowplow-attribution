{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}


{{
  config(
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    enabled=var('snowplow__enable_paths_to_non_conversion')
  )
}}

-- Requires macro trim_long_path


with non_conversions as (

  select
    customer_id,
    max(start_tstamp) as non_cv_tstamp

  from {{ var('snowplow_path_source') }} s

  where not exists (select customer_id from {{ ref('snowplow_attribution_conversions') }} c where s.customer_id = c.customer_id)
  and start_tstamp >= '{{ var("snowplow_attribution_start_date") }}'

  group by 1

)

, string_aggs as (

  select
    n.customer_id,
    {{ snowplow_utils.get_string_agg('channel', 's', separator=' > ', order_by_column='start_tstamp', sort_numeric=false, order_by_column_prefix='s') }} as path

  from non_conversions n

  inner join {{ var('snowplow_path_source') }} s
  on n.customer_id = s.customer_id
    and {{ datediff('s.start_tstamp', 'n.non_cv_tstamp', 'day') }}  >= 0
    and {{ datediff('s.start_tstamp', 'n.non_cv_tstamp', 'day') }} <= {{ var('snowplow__path_lookback_days') }}

  group by 1


)

{% if target.type not in ('redshift') %}

, arrays as (

    select
      customer_id,
      {{ snowplow_utils.get_split_to_array('path', 's', ' > ') }} as path,
      {{ snowplow_utils.get_split_to_array('path', 's', ' > ') }} as transformed_path

    from string_aggs s

)

{{ transform_paths('non_conversions', 'arrays') }}

select
  customer_id,
  {{ snowplow_utils.get_array_to_string('path', 'p', ' > ') }} as path,
  {{ snowplow_utils.get_array_to_string('transformed_path', 'p', ' > ') }} as transformed_path

from path_transforms p

{% else %}

, strings as (

  select
    customer_id,
    path as path,
    path as transformed_path

  from string_aggs s

)

  {{ transform_paths('non_conversions', 'strings') }}


select *
from path_transforms p

{% endif %}

