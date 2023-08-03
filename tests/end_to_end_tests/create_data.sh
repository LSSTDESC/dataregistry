#!/bin/bash

# Make some dummy data that is already in the registry root dir but not entered
# in the database. This is to test entering data with old_location = None.

mkdir -p $DREGS_ROOT_DIR/user/end_to_end_tests/dummy_dir
touch $DREGS_ROOT_DIR/user/end_to_end_tests/dummy_dir/file1.txt
touch $DREGS_ROOT_DIR/user/end_to_end_tests/dummy_dir/file2.txt

touch $DREGS_ROOT_DIR/user/end_to_end_tests/dummy_file1.txt
