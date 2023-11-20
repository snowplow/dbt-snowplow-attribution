{#
Copyright (c) 2024-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}


/* Macro to perform channel classifications
   each channel should return a name that will also be a valid column name by convention use underscores, avoid spaces,
   leading numbers) */

{% macro channel_classification() %}
    {{ return(adapter.dispatch('channel_classification', 'snowplow_attribution')()) }}
{% endmacro %}

{% macro default__channel_classification() %}

  default_channel_group

{% endmacro %}
