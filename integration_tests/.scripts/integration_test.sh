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
declare -a SUPPORTED_DATABASES=("bigquery" "databricks"  "snowflake")

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

  echo "Snowplow Attribution integration tests: Execute events_stg for unified package"

  eval "dbt run --select snowplow_attribution_events_stg --full-refresh --target $db" || exit 1;

  echo "Snowplow Unified: Execute models"

  eval "dbt run --select snowplow_unified --full-refresh --vars '{snowplow__allow_refresh: true}' --target $db" || exit 1;

  echo "Snowplow Attribution integration tests: Execute attribution models"

  eval "dbt run --select snowplow_attribution --full-refresh --target $db" || exit 1;

  echo "Snowplow Attribution integration tests: Execute attribution integration test models"

  eval "dbt run --select snowplow_attribution_integration_tests --full-refresh --target $db" || exit 1;

  echo "Snowplow Attribution integration tests: Test models"

  eval "dbt test --exclude snowplow_unified  --target $db" || exit 1;

  echo "Snowplow Attribution integration tests: All tests passed"

done
