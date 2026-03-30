import os

CLIENT_ID = "b10d6ef9-67f2-4dbb-830c-4b68673e601f"
NERSC_COLLECTION_ID = "9d6d994a-6d04-11e5-ba46-22000b92c6ec"

# A name for your application. This is good practice.
# Here use the name from Globus native app registration
APP_NAME = "lsstdesc-dataregistry-api"


# The path where tokens will be stored. NativeApp will handle this file.
TOKEN_FILE = os.path.expanduser("~/.globus/dataregistry_tokens.json")

__all__ = ["CLIENT_ID", "NERSC_COLLECTION_ID", "APP_NAME", "TOKEN_FILE"]
