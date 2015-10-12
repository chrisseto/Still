import abc
import datetime
from enum import Enum
from collections.abc import Awaitable

from asyncio_mongo import bson
from dateutil.parser import parse

from wdim.orm import query


class Field(metaclass=abc.ABCMeta):

    def __init__(self, index=False, unique=False, required=True):
        if unique:
            index = True

        self._name = None
        self.index = index
        self.unique = unique
        # TODO implement
        self.required = required

    @abc.abstractmethod
    def parse(self, value):
        raise NotImplemented

    def to_document(self, value):
        return value

    def get_value(self, inst):
        return inst._data.get(self._name)

    def set_value(self, instance, value):
        instance._data[self._name] = value

    def __get__(self, inst, owner):
        assert self._name is not None, 'Field must be attached to a class'

        # inst being None indicates that we're being accessed
        # by the class we're attached to, return self for ease of use
        if inst is None:
            return self

        return self.get_value(inst)

    def __set__(self, instance, value, override=False):
        assert self._name is not None, 'Field must be attached to a class'

        if not override:
            raise ValueError('Fields are read-only')
        return self.set_value(instance, value)

    def __eq__(self, value):
        return query.Equals(self, value)

    def __ne__(self, value):
        pass

    def __lt__(self, value):
        pass

    def __gt__(self, value):
        pass

    def __ge__(self, value):
        pass

    def __le__(self, value):
        pass


class ObjectIdField(Field):

    def parse(self, value):
        if value is None:
            return bson.ObjectId()
        if isinstance(value, bson.ObjectId):
            return value
        return bson.ObjectId(value)


class StringField(Field):

    def parse(self, value):
        if value is None:
            return ''
        return str(value)


class BoolField(Field):

    def parse(self, value):
        return bool(value)


class DictField(Field):

    def parse(self, value):
        if value is None:
            return {}
        return dict(value)


class DatetimeField(Field):

    def parse(self, value):
        if value is None and not self.required:
            return None
        if isinstance(value, str):
            return parse(value)
        if isinstance(value, datetime.datetime):
            return value
        raise Exception('{} must be str or datetime, got {}'.format(self._name, value))


class EnumField(Field):

    def __init__(self, enum, **kwargs):
        assert issubclass(enum, Enum), 'enum must be of type Enum, got {!r}'.format(enum)
        self._enum = enum
        super().__init__(**kwargs)

    def parse(self, value):
        self._enum(value)
        return value

    def to_document(self, value):
        return value.value


def AwaitableLoaderFactory(klass, type_, *args, **kwargs):
    assert isinstance(klass, type), 'klass must be a type, got {!r}'.format(klass)

    if getattr(type_, '_original_class', None):
        return args[0]

    class _Awaitable(type_, Awaitable):
        _is_coroutine = True
        _original_class = type_

        def __await__(self):
            return klass.load(self).__await__()

        __iter__ = __await__

    return _Awaitable(*args, **kwargs)


class ForeignField(Field):

    @property
    def foreign_class(self):
        return self.class_getter()

    def __init__(self, class_getter, **kwargs):
        self.class_getter = class_getter
        super().__init__(**kwargs)

    def parse(self, value):
        if value is None and not self.required:
            return None

        if isinstance(value, self.foreign_class):
            return value._id

        if isinstance(value, dict):
            value = value['_id']

        return self.foreign_class._id.parse(value)

    def get_value(self, inst):
        _id = super().get_value(inst)
        if _id is None:
            return None
        return AwaitableLoaderFactory(self.foreign_class, type(_id), _id)
