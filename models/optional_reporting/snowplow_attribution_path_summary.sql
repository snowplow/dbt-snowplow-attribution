{#
Copyright (c) 2024-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}


{{
  config(
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

with paths_to_conversion as (

  select
    'channel' as path_type,
    channel_transformed_path as transformed_path,
    count(*) as conversions,
    sum(revenue) as revenue

  from {{ ref('snowplow_attribution_paths_to_conversion') }}

  group by 1,2
  
  union all
  
  select
    'campaign' as path_type,
    campaign_transformed_path as transformed_path,
    count(*) as conversions,
    sum(revenue) as revenue

  from {{ ref('snowplow_attribution_paths_to_conversion') }}

  group by 1,2
)

{% if var('snowplow__enable_paths_to_non_conversion') %}

  , paths_to_non_conversion as (

    select
      'channel' as path_type,
      channel_transformed_path as transformed_path,
      count(*) as non_conversions

    from {{ ref('snowplow_attribution_paths_to_non_conversion') }}

    group by 1,2
    
    union all
    
    select
      'campaign' as path_type,
      campaign_transformed_path as transformed_path,
      count(*) as non_conversions

    from {{ ref('snowplow_attribution_paths_to_non_conversion') }}

    group by 1,2
  )

{% endif %}

select
  coalesce(c.path_type, null {% if var('snowplow__enable_paths_to_non_conversion') %}, n.path_type{% endif %}) as path_type,
  coalesce(c.transformed_path, null {% if var('snowplow__enable_paths_to_non_conversion') %}, n.transformed_path {% endif %}) as transformed_path,
  coalesce(c.conversions, 0) as conversions,
  
  {% if var('snowplow__enable_paths_to_non_conversion') %}
    coalesce(n.non_conversions, 0) as non_conversions,
  {% endif %}
  
  c.revenue

from paths_to_conversion c

{% if var('snowplow__enable_paths_to_non_conversion') %}
  full join paths_to_non_conversion n
    on c.transformed_path = n.transformed_path
{% endif %}

order by 1,3 desc
