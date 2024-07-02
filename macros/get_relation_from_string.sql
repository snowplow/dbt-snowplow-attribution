{#
Copyright (c) 2024-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

/*  Macro to allow for hardcoding of the source table reference in var('snowplow__conversion_path_source') 
and var('snowplow__conversions_source'). */

{% macro get_relation_from_string(full_table_reference) %}

  {% set full_table_reference_without_quotes = full_table_reference | replace("`", "") | replace('"', '') %}
  {% set parts = full_table_reference_without_quotes.split('.') %}
  
  {% if parts | length == 3 %}
    {% set database = parts[0] %}
    {% set schema = parts[1] %}
    {% set table = parts[2] %}
  
  {% elif parts | length == 2 %}
    {% set database = target.database %}
    {% set schema = parts[0] %}
    {% set table = parts[1] %}
    
  {% else %}
    {{ exceptions.raise_compiler_error(
"Snowplow Error: Compilation error within "~full_table_reference ~". Please make sure you provide a valid table reference that includes a schema."
    ) }}
  {% endif %}

  {% set relation = adapter.get_relation(
        database=database,
        schema=schema,
        identifier=table
    ) %}
  
  {{ return(relation) }}
{% endmacro %}
