{#
Copyright (c) 2024-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}


/* Define a conditional (where clause) that filters conversion paths based on the conversion hosts.
   If snowplow_unified_sessions is used instead of the defaulted snowplow_unified_views 
   then the field reference should be changed to first_url_host */

{% macro conversion_hosts_clause() %}
    {{ return(adapter.dispatch('conversion_clause', 'snowplow_attribution')()) }}
{% endmacro %}

{% macro default__conversion_hosts_clause() %}
  and page_urlhost in ({{ snowplow_utils.print_list(var('snowplow__conversion_hosts')) }})
{% endmacro %}
