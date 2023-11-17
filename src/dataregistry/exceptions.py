__all__ = ["DataRegistryException", "DataRegistryNYI"]


class DataRegistryException(Exception):
    pass


class DataRegistryNYI(DataRegistryException):
    def __init__(self, feature=""):

        msg = f"Feature {feature} not yet implemented"
        self.msg = msg
        super().__init__(self.msg)
