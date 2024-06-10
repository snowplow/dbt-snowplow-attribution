#!/bin/bash

# Expected input:
# -d (database) target database for dbt

while getopts 'd:' opt
do
  case $opt in
    d) DATABASE=$OPTARG
  esac
done

declare -a ATTRIBUTION_MODELS_TO_TEST=("last_touch" "shapley")
declare -a SUPPORTED_DATABASES=("bigquery" "databricks"  "snowflake", "redshift")

# set to lower case
DATABASE="$(echo $DATABASE | tr '[:upper:]' '[:lower:]')"

if [[ $DATABASE == "all" ]]; then
  DATABASES=( "${SUPPORTED_DATABASES[@]}" )
else
  DATABASES=$DATABASE
fi

for db in ${DATABASES[@]}; do

  echo "Snowplow Attribution integration tests: Seeding data"

  eval "dbt seed --full-refresh --target $db" || exit 1;
  
  echo "Snowplow Attribution integration tests: Execute events_stg for unified package and the spend source"

  eval "dbt run --select snowplow_unified_events_stg spend_source --full-refresh --target $db" || exit 1;

  if [[ $db == "redshift" ]]; then
  
  echo "Snowplow Attribution integration tests: Execute page_view_context_stg for Redshift only"

  eval "dbt run --select snowplow_unified_page_view_context_stg --full-refresh --target $db" || exit 1;
  
  fi
  
  echo "Snowplow Unified: Execute models - 1/2"

  eval "dbt run --select snowplow_unified --full-refresh --vars '{snowplow__allow_refresh: true}' --target $db" || exit 1;

  echo "Snowplow Attribution integration tests: Execute attribution models - 1/2"

  eval "dbt run --select snowplow_attribution --full-refresh --vars '{snowplow__allow_refresh: true}' --target $db" || exit 1;

  echo "Snowplow Unified: Execute models - 2/2"

  eval "dbt run --select snowplow_unified --target $db" || exit 1;

  echo "Snowplow Attribution integration tests: Execute attribution models - 2/2"

  eval "dbt run --select snowplow_attribution --target $db" || exit 1;

  echo "Snowplow Attribution integration tests: Execute attribution integration test models"

  eval "dbt run --select snowplow_attribution_integration_tests --full-refresh --target $db" || exit 1;
  
  if [[ $db == "redshift" ]]; then
  
  echo "Snowplow Attribution integration tests: Execute attribution_overview for redshift only"

  eval "dbt run --select snowplow_attribution_overview --target $db" || exit 1;

  fi

  echo "Snowplow Attribution integration tests: Test models"

  eval "dbt test --exclude snowplow_unified  --target $db" || exit 1;

  echo "Snowplow Attribution integration tests: All tests passed"

done
