#!/bin/bash

# A basic entry
dregs register dataset my_cli_dataset "0.0.1" \
    --is_dummy
dregs register dataset my_cli_dataset2 "patch" \
    --is_dummy \
    --name my_cli_dataset

# A basic entry with more options
dregs register dataset my_cli_dataset3 "1.2.3" --is_dummy \
    --description "This is my dataset description" \
    --access_API "Awesome API" \
    --owner "DESC" \
    --owner_type "group" \
    --version_suffix "test" \
    --creation_date "2020-01-01" \
    --input_datasets 1 2 \
    --execution_name "I have given the execution a name" \
    --is_overwritable

# A production dataset
if [ "$DATAREG_BACKEND" = "postgres" ]; then
  dregs register dataset my_production_cli_dataset "0.1.2" \
      --owner_type "production" \
      --is_dummy \
      --schema "production"
fi
