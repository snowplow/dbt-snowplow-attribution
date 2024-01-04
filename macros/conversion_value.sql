{#
Copyright (c) 2024-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}


/* Enter a single column (or calculated value) for the value assigned
   to a given conversion event. This column should be a float / double.
   by default we use tr_total but this may be something like
   com_acme_checkout_1[SAFE_OFFSET(0)].cart_value
   Do not alias this value */

{% macro conversion_value() %}
    {{ return(adapter.dispatch('conversion_value', 'snowplow_attribution')()) }}
{% endmacro %}

{% macro default__conversion_value() %}
    ev.cv_value
{% endmacro %}
