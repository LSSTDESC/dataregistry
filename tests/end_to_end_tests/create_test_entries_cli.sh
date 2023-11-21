#!/bin/bash

# A basic entry
dregs register dataset my_cli_dataset "0.0.1" \
    --is_dummy \
    --root_dir "DataRegistry_data"
dregs register dataset my_cli_dataset2 "patch" \
    --is_dummy \
    --name my_cli_dataset \
    --root_dir "DataRegistry_data"

# A basic entry with more options
dregs register dataset my_cli_dataset3 "1.2.3" --is_dummy \
    --description "This is my dataset description" \
    --access_API "Awesome API" \
    --locale "Secret location" \
    --owner "DESC" \
    --owner-type "group" \
    --version_suffix "test" \
    --root_dir "DataRegistry_data"

# A production dataset
dregs register dataset my_production_cli_dataset "0.1.2" \
    --owner-type "production" \
    --is_dummy \
    --root_dir "DataRegistry_data"
