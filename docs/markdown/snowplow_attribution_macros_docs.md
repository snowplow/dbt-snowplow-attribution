{% docs macro_channel_classification %}

{% raw %}
A macro used to perform channel classifications. Each channel should be classified a name that is a valid field name as it will be used for that purpose, once unnested downstream.

#### Returns

A sql of case statements that determine which channel is classified (it is most likely unique to each organisation, the sample provided is based on Google's Attribution).

Example:
```sql
    case when lower(mkt_medium) in ('cpc', 'ppc') and regexp_count(lower(mkt_campaign), 'brand') > 0 then 'Paid_Search_Brand'
         when lower(mkt_medium) in ('cpc', 'ppc') and regexp_count(lower(mkt_campaign), 'generic') > 0 then 'Paid_Search_Generic'
         when lower(mkt_medium) in ('cpc', 'ppc') and not regexp_count(lower(mkt_campaign), 'brand|generic') > 0 then 'Paid_Search_Other'
         when lower(mkt_medium) = 'organic' then 'Organic_Search'
         when lower(mkt_medium) in ('display', 'cpm', 'banner') and regexp_count(lower(mkt_campaign), 'prospect') > 0 then 'Display_Prospecting'
         when lower(mkt_medium) in ('display', 'cpm', 'banner') and regexp_count(lower(mkt_campaign), 'retargeting|re-targeting|remarketing|re-marketing') > 0 then 'Display_Retargeting'
         when lower(mkt_medium) in ('display', 'cpm', 'banner') and not regexp_count(lower(mkt_campaign), 'prospect|retargeting|re-targeting|remarketing|re-marketing') > 0 then 'Display_Other'
         when regexp_count(lower(mkt_campaign), 'video|youtube') > 0 or regexp_count(lower(mkt_source), 'video|youtube') > 0 then 'Video'
         when lower(mkt_medium) = 'social' and regexp_count(lower(mkt_campaign), 'prospect') > 0 then 'Paid_Social_Prospecting'
         when lower(mkt_medium) = 'social' and regexp_count(lower(mkt_campaign), 'retargeting|re-targeting|remarketing|re-marketing') > 0 then 'Paid_Social_Retargeting'
         when lower(mkt_medium) = 'social' and not regexp_count(lower(mkt_campaign), 'prospect|retargeting|re-targeting|remarketing|re-marketing') > 0 then 'Paid_Social_Other'
         when mkt_source = '(direct)' then 'Direct'
         when lower(mkt_medium) = 'referral' then 'Referral'
         when lower(mkt_medium) = 'email' then 'Email'
         when lower(mkt_medium) in ('cpc', 'ppc', 'cpv', 'cpa', 'affiliates') then 'Other_Advertising'
         else 'Unmatched_Channel'
    end
```

#### Usage

```sql

select {{ channel_classification() }} as channel,

```
{% endraw %}
{% enddocs %}


{% docs macro_create_udfs %}

{% raw %}
Creates user defined functions for adapters apart from Databricks. It is executed as part of an on-start hook.

#### Returns

Nothing, sql is executed which creates the UDFs in the target database and schema.

#### Usage

```yml
-- dbt_project.yml
...
on-run-start: "{{ create_udfs() }}"
...

```
{% endraw %}
{% enddocs %}



{% docs macro_attribution_overview %}

{% raw %}
Defines the sql for the view called overview.

#### Returns

The sql required to create the view called overview.

#### Usage

```yml
{{ attribution_overview() }}
```
{% endraw %}
{% enddocs %}


{% docs macro_source_checks %}

{% raw %}
 Macro to make sure the user does not execute the package with missing conversion or path source data.

#### Returns

Error message, if it fails one of the checks.


#### Usage

```sql

{{ source_checks() }}

```
{% endraw %}
{% enddocs %}


{% docs macro_path_transformation %}

{% raw %}
 Macro to execute the indvidual path_transformation specified as a parameter.

#### Returns

The transformed array column.


#### Usage

```sql

{{ path_transformation('unique_path') }} as transformed_path

```
{% endraw %}
{% enddocs %}


{% docs macro_transform_paths %}

{% raw %}
Macro to remove complexity from models paths_to_conversion / paths_to_non_conversion.

#### Returns

The sql with the missing cte's that take care of path transformations.

#### Usage

It is used by the transform_paths() macro for the transformation cte sql code build. It takes a transformation type as a parameter and its optional argument, if exists. The E.g.

```sql
with base_data as (...),

{{ transform_paths('conversions', 'base_data') }}

select * from path_transforms
```

{% endraw %}
{% enddocs %}


{% docs macro_trim_long_path %}

{% raw %}
Returns the last 'snowplow__path_lookback_steps' number of channels in the path if snowplow__path_lookback_steps > 0, or the full path otherwise.

#### Returns

The transformed array column.


#### Usage

```sql

select
  ...
  {{ trim_long_path('path', var('snowplow__path_lookback_steps')) }} as path,
  ...
from
  ...

```
{% endraw %}
{% enddocs %}


{% docs macro_allow_refresh %}
{% raw %}
This macro is used to determine if a full-refresh is allowed (depending on the environment), using the `snowplow__allow_refresh` variable.

#### Returns
`snowplow__allow_refresh` if environment is not `dev`, `none` otherwise.

{% endraw %}
{% enddocs %}

{% docs macro_paths_to_conversion %}

{% raw %}
Macro to allow flexibility for users to modify the definition of the paths_to_conversion incremental table.
#### Returns

The sql to define the paths_to_conversion table.

#### Usage

```sql

{{ paths_to_conversion() }}

```

{% endraw %}
{% enddocs %}
