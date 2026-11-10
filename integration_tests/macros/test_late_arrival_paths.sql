{#
Copyright (c) 2024-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{#
Asserts the state of paths_to_conversion after the late-arrival scenario in integration_test.sh:
  run 1 (full refresh): only cv_anchor (2024-03-10 12:00) exists, so it becomes the anchor.
  run 2 (incremental): the rest arrive. With snowplow__days_late_allowed = 3 the window is 96h,
  so conversions from 2024-03-06 12:00 onwards are (re)processed.
#}

{% macro test_late_arrival_paths() %}
  {% if execute %}
    {% set expected = {
      'cv_anchor': 'Organic_Search',
      'cv_late_2d': 'Email > Direct',
      'cv_boundary': 'Referral > Direct',
      'cv_new': 'Paid_Search_Other'
    } %}
    {#- Channels come from the integration project's channel_classification (mkt_medium / mkt_source).
        cv_late_2d: arrived 2 days late, must be picked up (it was lost with the 6h window).
        cv_boundary: earliest conversion in the window; its Referral visit sits on the boundary day
          (30 days before the conversion date), so it checks the path floor is not too tight. Display_Other,
          31 days before, must stay out.
        cv_too_late: arrived 5 days late, beyond days_late_allowed, so it is not processed. -#}

    {% set query %}
      select cv_id, channel_path
      from {{ ref('snowplow_attribution_paths_to_conversion') }}
      order by cv_id
    {% endset %}
    {% set actual = {} %}
    {% for row in run_query(query).rows %}
      {% do actual.update({row[0]: row[1]}) %}
    {% endfor %}

    {% set failures = [] %}
    {% for cv_id, path in expected.items() %}
      {% if cv_id not in actual %}
        {% do failures.append(cv_id ~ ' is missing (expected path ' ~ path ~ ')') %}
      {% elif actual[cv_id] != path %}
        {% do failures.append(cv_id ~ ' expected path ' ~ path ~ ', got ' ~ actual[cv_id]) %}
      {% endif %}
    {% endfor %}
    {% for cv_id in actual %}
      {% if cv_id not in expected %}
        {% do failures.append(cv_id ~ ' should not have been processed, got path ' ~ actual[cv_id]) %}
      {% endif %}
    {% endfor %}

    {% if failures %}
      {{ exceptions.raise_compiler_error('Late-arrival incremental regression: ' ~ (failures | join('; '))) }}
    {% endif %}
    {% do log('Late-arrival incremental tests passed (' ~ (expected | length) ~ ' conversions).', info=true) %}
  {% endif %}
{% endmacro %}
