class WdimError(Exception):
    pass


class ClientError(WdimError):
    pass


class SchemaError(ClientError):
    pass


class ValidationError(SchemaError):
    pass


class NoSuchSchema(SchemaError):
    pass
