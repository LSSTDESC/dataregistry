__all__ = ["DataRegistryException", "DataRegistryNYI", "DataRegistryRootDirBadState"]


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
