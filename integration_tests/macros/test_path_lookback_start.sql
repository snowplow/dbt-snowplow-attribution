{#
Copyright (c) 2024-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% macro test_path_lookback_start() %}
  {% if execute %}
    {# Literal calendar boundaries exercise the production helper without reproducing its arithmetic. #}
    {% set cases = [
      ('fall_back', '2026-11-03 06:00:00', 30, '2026-10-04 00:00:00'),
      ('spring_forward', '2026-03-10 06:00:00', 30, '2026-02-08 00:00:00'),
      ('zero_days', '2026-11-03 12:00:00', 0, '2026-11-03 00:00:00'),
      ('previous_day', '2026-11-02 20:00:00', 30, '2026-10-03 00:00:00')
    ] %}
    {% set change_timezone = target.type in ['spark', 'databricks'] %}
    {% if change_timezone %}
      {% set original_timezone = run_query('select current_timezone()').rows[0][0] %}
      {% set timezones = ['America/New_York', 'UTC'] %}
    {% else %}
      {% set timezones = ['default'] %}
    {% endif %}
    {% set failures = [] %}

    {% for timezone in timezones %}
      {% if change_timezone %}
        {% call statement('set_path_test_timezone', fetch_result=false, auto_begin=false) %}
          set time zone '{{ timezone }}'
        {% endcall %}
      {% endif %}

      {% set test_sql %}
        with bounds as (
          {% for label, earliest_cv, days, expected in cases %}
            select '{{ label }}' as test_case,
              {{ snowplow_attribution.get_path_lookback_start("cast('" ~ earliest_cv ~ "' as " ~ dbt.type_timestamp() ~ ")", days) }} as actual_start,
              cast('{{ expected }}' as {{ dbt.type_timestamp() }}) as expected_start
            {% if not loop.last %}union all{% endif %}
          {% endfor %}
        )
        select * from bounds
        where actual_start is null or actual_start <> expected_start
      {% endset %}
      {% for row in run_query(test_sql).rows %}
        {% do failures.append(timezone ~ ': ' ~ row[0] ~ ' expected ' ~ row[2] ~ ', got ' ~ row[1]) %}
      {% endfor %}
    {% endfor %}

    {# Restore the connection before reporting an assertion failure. SET has no result set on PyHive. #}
    {% if change_timezone %}
      {% call statement('restore_path_test_timezone', fetch_result=false, auto_begin=false) %}
        set time zone '{{ original_timezone | replace("'", "''") }}'
      {% endcall %}
    {% endif %}
    {% if failures %}
      {{ exceptions.raise_compiler_error('Path lookback boundary regression: ' ~ (failures | join('; '))) }}
    {% endif %}
    {% do log('Path lookback boundary tests passed (' ~ ((cases | length) * (timezones | length)) ~ ' cases).', info=true) %}
  {% endif %}
{% endmacro %}
