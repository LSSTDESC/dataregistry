__all__ = ["DataRegistryException", "DataRegistryNYI",
           "DataRegistryRootDirBadState", "dataRegistryNoEntry",
           "DataRegistryUnmanaged", "DataRegistryColumnSpec",
           "DataRegistryNoColumn"]


class DataRegistryException(Exception):
    pass


class DataRegistryNYI(DataRegistryException):
    def __init__(self, feature=""):
        msg = f"Feature {feature} not yet implemented"
        self.msg = msg
        super().__init__(self.msg)


class DataRegistryRootDirBadState(DataRegistryException):
    def __init__(self, error=""):
        msg = f"Found a bad state in the `root_dir`: {error}"
        self.msg = msg
        super().__init__(self.msg)


class DataRegistryNoEntry(DataRegistryException):
    def __init__(self, dataset_id="", schema_mode=""):
        msg = f"Dataset id {dataset_id}, schema_mode {schema_mode} "
        msg += "does not exist or was deleted"
        self.msg = msg
        super().__init__(self.msg)

class DataRegistryUnmanaged(DataRegistryException):
    def __init__(self, dataset_id="", schema_mode=""):
        msg = f"For dataset id {dataset_id}, schema_mode {schema_mode} "
        msg += "dataregistry does not manage data files, only metadata"
        self.msg = msg
        super().__init__(self.msg)

class DataRegistryColumnSpec(DataRegistryException):
    def __init__(self, column_spec=""):
        msg = f"More than one table has column {column_spec}.\n"
        msg += "Include table name in specification."
        self.msg = msg
        super().__init__(self.msg)

class DataRegistryNoColumn(DataRegistryException):
    def __init__(self, column_spec=""):
        msg = f"No such column as {column_spec}."
        self.msg = msg
        super().__init__(self.msg)
