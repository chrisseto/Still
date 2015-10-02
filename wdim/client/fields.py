import abc
import asyncio

from asyncio_mongo import bson


class Field(metaclass=abc.ABCMeta):

    def __init__(self, index=False, unique=False):
        if unique:
            index = True

        self.index = index
        self.unique = unique

    @abc.abstractmethod
    def parse(self, value):
        raise NotImplemented

    @abc.abstractmethod
    def to_document(self, value, join=False):
        raise NotImplemented

    def __get__(self, inst, owner):
        # inst being None indicates that we're being accessed
        # by the class we're attached to, return self for ease of use
        if inst is None:
            return self

        return self.get_value(inst)

    def __set__(self, instance, value, override=False):
        if not override:
            raise ValueError('Fields are read-only')
        return self.set_value(inst, value)

    def get_value(self, inst):
        for key, field in inst._fields.items():
            if field == self:
                return inst._data.get(key)
        raise Exception('Wat')

    def set_value(self, inst, value):
        for key, field in instance._fields.items():
            if field == self:
                instance._data[key] = value
                return
        raise Exception('Wat')


class ObjectIdField(Field):

    def parse(self, value):
        if value is None:
            return bson.ObjectId()
        if isinstance(value, bson.ObjectId):
            return value
        return bson.ObjectId(value)

    def to_document(self, value, join=False):
        return value


class StringField(Field):

    def parse(self, value):
        if value is None:
            return ''
        return str(value)

    def to_document(self, value, join=False):
        return value


class DictField(Field):

    def parse(self, value):
        if value is None:
            return {}
        return dict(value)

    def to_document(self, value):
        return value


class AsyncObjectId(bson.ObjectId):

    def __init__(self, *args, klass=None, **kwargs):
        assert isinstance(klass, type), 'klass must be a type'
        self._klass = klass
        super().__init__(*args, **kwargs)

    def __await__(self):
        return self._klass.load(self).__await__()


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

        assert isinstance(
            value,
            (str, bson.ObjectId, self.foreign_class)
        ), 'value must be a primary key or instance of {!r}, got {!r}'.format(self.foreign_class, value)

        if isinstance(value, (str, bson.ObjectId)):
            return value

        return value._id

    def get_value(self, inst):
        _id = super().get_value(inst)
        if _id is None:
            return None
        return AsyncObjectId(_id, klass=self.foreign_class)
