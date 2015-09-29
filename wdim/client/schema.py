import abc

import jsonschema

from wdim import exceptions


def get_schema(name):
    try:
        return next(x for x in BaseSchema.__subclasses__() if x.name == name)
    except StopIteration:
        raise exceptions.NoSuchSchema(name)


class BaseSchema(metaclass=abc.ABCMeta):

    @classmethod
    @abc.abstractmethod
    def validate_schema(self, schema):
        raise NotImplemented

    @property
    @abc.abstractmethod
    def name(self, schema):
        raise NotImplemented

    def __init__(self, _id, schema, version=None):
        self._id = _id
        if version is not None:
            self._id += '::' + version
        self.schema = schema

    def validate(self, data):
        raise NotImplemented


class JSONSchema(BaseSchema):
    name = 'jsonschema'

    @classmethod
    def validate_schema(self, schema):
        # TODO Translate to custom exceptions
        jsonschema.Draft4Validator.check_schema(schema)

    def validate(self, data):
        # TODO Translate to custom exceptions
        jsonschema.validate(data, self.schema)


__all__ = (
    'get_schema',
) + tuple(x.__class__.__name__ for x in BaseSchema.__subclasses__())
