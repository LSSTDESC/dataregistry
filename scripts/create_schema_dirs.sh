#!/bin/bash

# Script to create initial schema directories within the root_dir
# Must be run under the `descdr` group account

# Check if the correct number of arguments is provided
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <root_dir> <schema_name>"
    exit 1
fi

# Assign inputs to variables
BASE_DIR="$1"
FOLDER_NAME="$2"
TARGET_DIR="${BASE_DIR}/${FOLDER_NAME}"

# Check if the base directory exists
if [ ! -d "$BASE_DIR" ]; then
    echo "Error: Base directory '$BASE_DIR' does not exist."
    exit 1
fi

# Create the main folder in the base directory
mkdir -p "$TARGET_DIR"

# Check if the main folder was created successfully
if [ $? -ne 0 ]; then
    echo "Error: Could not create directory '$TARGET_DIR'."
    exit 1
fi

# Create subdirectories: user, group, project, production
mkdir -p "$TARGET_DIR/user" "$TARGET_DIR/group" "$TARGET_DIR/project" "$TARGET_DIR/production"

# Check if subdirectories were created successfully
if [ $? -ne 0 ]; then
    echo "Error: Could not create subdirectories."
    exit 1
fi

# Set the owning group to have read and execute (r-x) permissions on the main folder
chmod g=rx "$TARGET_DIR"

# Set read and execute ACL for the lsst group on the main folder
#setfacl -m g:lsst:rx "$TARGET_DIR"

# Check if the ACL was set successfully
if [ $? -eq 0 ]; then
    echo "Folder '$TARGET_DIR' with subdirectories created and ACL set for 'lsst' group."
else
    echo "Error: Failed to set ACL for the 'lsst' group."
    exit 1
fi
