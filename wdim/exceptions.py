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


class NotFound(ClientError):
    pass


class UniqueViolation(ClientError):
    pass


class UnsupportedOperation(ClientError):
    pass
