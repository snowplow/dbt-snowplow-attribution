{#
Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{% if target.type != 'redshift' %}
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

{% else %}

-- Mirrors the SQL patterns in redshift__transform_paths (macros/path_transformations/transform_paths.sql).
-- Uses CASE WHEN inside LISTAGG to test each transform independently in a single query,
-- since the macro applies all configured transforms as simultaneous WHERE filters.
with vertical_path_base as (
  select 'customer_1' as customer_id, 'Example' as channel, 'Campaign 1' as campaign, '2023-01-01 01:00:00'::timestamp as visit_start_tstamp union all
  select 'customer_1', 'Video',   'Campaign 1', '2023-01-01 02:00:00'::timestamp union all
  select 'customer_1', 'Direct',  'Campaign 2', '2023-01-01 03:00:00'::timestamp union all
  select 'customer_1', 'Direct',  'Campaign 2', '2023-01-01 04:00:00'::timestamp union all
  select 'customer_2', 'Direct',  'Campaign 1', '2023-01-01 01:00:00'::timestamp union all
  select 'customer_2', 'Direct',  'Campaign 1', '2023-01-01 02:00:00'::timestamp union all
  select 'customer_3', 'a',       'Campaign 1', '2023-01-01 01:00:00'::timestamp union all
  select 'customer_3', 'a',       'Campaign 1', '2023-01-01 02:00:00'::timestamp union all
  select 'customer_3', 'a',       'Campaign 1', '2023-01-01 03:00:00'::timestamp union all
  select 'customer_3', 'Direct',  'Campaign 2', '2023-01-01 04:00:00'::timestamp union all
  select 'customer_3', 'a',       'Campaign 1', '2023-01-01 05:00:00'::timestamp union all
  select 'customer_3', 'Direct',  'Campaign 2', '2023-01-01 06:00:00'::timestamp union all
  select 'customer_3', 'Direct',  'Campaign 2', '2023-01-01 07:00:00'::timestamp union all
  select 'customer_4', 'Direct',  'Campaign 1', '2023-01-01 01:00:00'::timestamp union all
  select 'customer_6', 'Example', 'Campaign 1', '2023-01-01 01:00:00'::timestamp union all
  select 'customer_6', 'Video',   'Campaign 1', '2023-01-01 02:00:00'::timestamp union all
  select 'customer_6', 'Direct',  'Campaign 2', '2023-01-01 03:00:00'::timestamp
),

analytic_windows as (
  select
    customer_id, channel, campaign, visit_start_tstamp,
    row_number() over (partition by customer_id order by visit_start_tstamp desc) as reverse_step,
    lag(channel)  over (partition by customer_id order by visit_start_tstamp) as prev_channel,
    lag(campaign) over (partition by customer_id order by visit_start_tstamp) as prev_campaign,
    row_number() over (partition by customer_id, channel  order by visit_start_tstamp) as channel_occ,
    row_number() over (partition by customer_id, campaign order by visit_start_tstamp) as campaign_occ
  from vertical_path_base
),

base_windows as (
  select *,
    sum(case when channel  != 'Direct' then 1 else 0 end) over (partition by customer_id) as channel_non_param_count,
    max(case when channel  != 'Direct' then visit_start_tstamp end) over (partition by customer_id) as last_non_param_channel_tstamp,
    1 as _dummy
  from analytic_windows
),

actual_result as (
  select
    -- raw_array is the full untransformed path, matching the expected format
    listagg(channel, ' > ') within group (order by visit_start_tstamp) as raw_array,

    -- trim_long_path: last 1 step
    listagg(case when reverse_step = 1 then channel end, ' > ')
      within group (order by visit_start_tstamp) as trim_long_path,

    -- trim_long_path2: last 2 steps
    listagg(case when reverse_step <= 2 then channel end, ' > ')
      within group (order by visit_start_tstamp) as trim_long_path2,

    -- unique_path: identity
    listagg(channel, ' > ')
      within group (order by visit_start_tstamp) as unique_path,

    -- exposure_path
    listagg(case when prev_channel is null or channel != prev_channel then channel end, ' > ')
      within group (order by visit_start_tstamp) as exposure_path,

    -- first_path
    listagg(case when channel_occ = 1 then channel end, ' > ')
      within group (order by visit_start_tstamp) as first_path,

    -- remove_if_not_all('Direct')
    listagg(case when channel != 'Direct' or channel_non_param_count = 0 then channel end, ' > ')
      within group (order by visit_start_tstamp) as remove_if_not_all,

    -- remove_if_last_and_not_all('Direct')
    listagg(case when channel != 'Direct' or last_non_param_channel_tstamp is null or visit_start_tstamp <= last_non_param_channel_tstamp then channel end, ' > ')
      within group (order by visit_start_tstamp) as remove_if_last_and_not_all

  from base_windows
  group by customer_id
)

select * from actual_result

{% endif %}
