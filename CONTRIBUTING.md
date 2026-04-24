# Contributing to `dbt-snowplow-*`

This document is a guide for making code changes to `dbt-snowplow-*` packages. It assumes familiarity with dbt package development and basic Python tooling (virtualenvs, `pip`). Specific commands assume macOS or Linux.

1. [Contributing to `dbt-snowplow-*`](#contributing-to-dbt-snowplow-)
   1. [Setting up an environment](#setting-up-an-environment)
   2. [Implementation guidelines](#implementation-guidelines)
   3. [Testing](#testing)
   4. [Adding a CHANGELOG entry](#adding-a-changelog-entry)
   5. [Submitting a Pull Request](#submitting-a-pull-request)


## Setting up an environment

Assuming you already have dbt installed, it will be beneficial to create a profile for any warehouse connections you have when it comes to testing the changes to your package. The easiest way to do this that will involve the least changes to the testing setup is to create an `integration_tests` profile and populate it with any connections you have to our supported warehouse types (redshift+postgres, databricks, snowflake, bigquery). 

**It is recommended you use a custom schema for integration tests.**

```yml
integration_tests:
  outputs:
    databricks:
      type: databricks
      ...
    snowflake:
      type: snowflake
      ...
    bigquery:
      type: bigquery
      ...
    redshift:
      type: redshift
      ...
    postgres:
      type: postgres
      ...
  target: postgres
```

## Implementation guidelines

In general we try to follow these rules of thumb, but there are possible exceptions:
- Dispatch any macro where it needs to support multiple warehouses. 
  - Use inheritance where possible i.e. only define a macro for `redshift` if it is different to `postgres`, the same for `databricks` and `spark`
- Where models need to be different across multiple warehouse types, ensure they are enabled based on the `target.type`
- Make use of macros (ours and dbt's) where possible to avoid duplication and to manage the differences between warehouses
  - Do not reinvent the wheel e.g. make use of [`type_*` macros](https://docs.getdbt.com/reference/dbt-jinja-functions/cross-database-macros#data-type-functions) instead of explicit datatypes
  - In the case where a macro may be useful outside of a specific package, we may make the choice to add it to `dbt-snowplow-utils` [repository](https://www.github.com/snowplow/dbt-snowplow-utils) instead
- Make use of the incremental logic as much as possible to avoid full-scanning large tables
- Where new functionality is being added, or you are touching existing functionality that does not have good/any test, add tests

## Testing

Once you're able to manually test that your code change is working as expected, it's important to run existing automated tests, as well as adding some new ones. These tests will ensure that:
- Your code changes do not unexpectedly break other established functionality
- Your code changes can handle all known edge cases
- The functionality you're adding will _keep_ working in the future

In general our packages all have similar structures, with an `integration_tests` folder that contains a `.scripts/integration_tests.sh` file. This script is run with 1 argument, the name of your `target` in the `integration_tests` profile e.g. `./integration_tests/.scripts/integration_tests.sh -d postgres` which will run all the tests on your postgres instance. This all means you don't need your own Snowplow data to run the tests.

Tests are of 1 of 2 kinds:
- Row count/equality tests; these ensure that the processed seed data from the package matches exactly an expected input seed file. If you have made no change to logic these should not fail, however if you have changed the logic you may need to edit the expected seed file, and add records to the events input seed file to cover the use case. In some cases it may make sense to add both expected and unexpected data to the test (i.e. to ensure a fix you have deployed actually fixes the issue you have seen).
- Macro based tests; these are more varied, sometimes checking the output sql from a macro or otherwise examining database objects. Look at existing tests for more details and for how to edit/create these.

To run the integration tests:
1. Ensure the `integration_tests` folder is your working directory (you may need to `cd integration_tests`)
2. Run `dbt run-operation post_ci_cleanup` to ensure a clean set of schemas (this will drop the schemas we use, so ensure your profile is only for these tests)
3. Run `./.scripts/integration_test.sh -d {target}` with your target name
4. Ensure all tests run successfully

If any tests fail, you should examine the outputs and either correct the test or correct your changes.

> If you do not have access to all warehouses do not worry, test what you can and the remainder will be run when you submit your Pull Request (once enabled by maintainers).

This package has additional steps as the data is processed later by the python scripts, if you are making changes to these scripts please do your best to test them and make any changes to the python scripts rather than the docker image.

For specific details for running existing integration tests and adding new ones to this package see [integration_tests/README.md](integration_tests/README.md).

## Adding CHANGELOG Entry

You don't need to worry about which version your change will go into. Just create the changelog entry at the top of CHANGELOG.md, copying the style of those below, but populate the date and version numbers with `x`s and open your Pull Request against the `main` branch.

## Submitting a Pull Request

Open the PR against `main`. Flag in the description whether you believe the change is a patch, minor, or major release (if unsure, say so). Automated tests run via GitHub Actions; once they pass and the PR is approved, a maintainer will merge it into the active development branch.
