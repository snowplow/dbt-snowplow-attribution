{#
Copyright (c) 2024-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}


{% if var('snowplow__run_python_script_in_snowpark') %}
select *

from {{ source('python_created_tables', 'snowplow_attribution_channel_attribution') }}
-- Using source() here to avoid a node error when the table is not found in the models/ folder (as it is created by the python script, not dbt). 
{% else %}
-- For non-snowpark use cases just need a dummy select as no tests will be run on these models
select 1 as col
{% endif %}
