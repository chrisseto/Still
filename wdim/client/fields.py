import abc

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

        for key, value in inst._fields.items():
            if value == self:
                return inst._data.get(key)
        raise Exception('Wat')

    def __set__(self, instance, value, override=False):
        if not override:
            raise ValueError('Fields are read-only')

        for key, value in instance._fields.items():
            if value == self:
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
        return str(value)

    def to_document(self, value, join=False):
        return value


class ForeignField(Field):

    @property
    def foreign_class(self):
        return self.class_getter()

    def __init__(self, class_getter, **kwargs):
        self.class_getter = class_getter
        super().__init__(**kwargs)

    def parse(self, value):
        assert isinstance(value, (str, self.foreign_class)), 'value must be a primary key or instance of {!r}'.format(self.foreign_class)

        if isinstance(value, str):
            pass

        return value

    def to_document(self, value, join=False):
        if join:
            return value.to_document()
        return value._id
