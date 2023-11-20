{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}


/* Define a conditional (where clause) that filters to conversion events
   by default we use tr_total but this may refer to an entity
   or event (e.g., event_name = 'checkout') */

{% macro conversion_clause() %}
    {{ return(adapter.dispatch('conversion_clause', 'snowplow_attribution')()) }}
{% endmacro %}

{% macro default__conversion_clause() %}
    cv_value > 0
{% endmacro %}
