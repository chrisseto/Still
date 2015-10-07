class ORMError(Exception):
    pass


class NotFound(ORMError):
    pass


class UniqueViolation(ORMError):
    pass


class UnsupportedOperation(ORMError):
    pass
