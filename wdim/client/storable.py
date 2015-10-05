import abc

from wdim.util import pack
from wdim import exceptions
from wdim.client import fields


class StorableMeta(abc.ABCMeta):

    STORABLE_CLASSES = {}

    def __init__(cls, name, bases, dct):
        super().__init__(name, bases, dct)

        cls._collection_name = cls.__name__.lower()

        cls._fields = {}

        for key, value in cls.__dict__.items():
            if not isinstance(value, fields.Field):
                continue

            value._name = key
            cls._fields[key] = value

        cls._fields = {
            key: value
            for key, value
            in cls.__dict__.items()
            if isinstance(value, fields.Field)
        }

        if cls.__name__ != 'Storable':
            assert '_id' in cls._fields, '_id must be specified'
            StorableMeta.STORABLE_CLASSES[cls._collection_name] = cls


class _ConnectionAssertion:

    def __get__(self, inst, owner):
        raise ValueError('connect must be called before interacting with the database')


class Storable(metaclass=StorableMeta):

    _DATABASE = _ConnectionAssertion()

    @classmethod
    def ClassGetter(cls, name):
        def getter():
            try:
                return StorableMeta.STORABLE_CLASSES[name.lower()]
            except KeyError:
                raise KeyError('Class {} not found'.format(name))
        return getter

    @classmethod
    async def _bootstrap(cls):
        for klass in Storable.__subclasses__():
            indices = [
                pack(key, unique=value.unique, order=1)
                for key, value in klass._fields.items()
                if value.index
            ]
            if getattr(klass, 'Meta', None) and getattr(klass.Meta, 'indexes', None):
                indices.extend(klass.Meta.indexes)

            await cls._DATABASE.ensure_index(klass._collection_name, indices)

        return True

    @classmethod
    async def connect(cls, db, bootstrap=True):
        Storable._DATABASE = db
        if bootstrap:
            return await Storable._bootstrap()
        return True

    @classmethod
    async def create(cls, *, _id=None, **kwargs):
        if _id:
            inst = cls(_id=_id, **kwargs)
        else:
            inst = cls(**kwargs)

        db_id = await cls._DATABASE.insert(inst)

        if _id:
            assert _id == db_id, 'Database _id did not match given _id'

        cls._fields['_id'].__set__(inst, db_id, override=True)

        return inst

    @classmethod
    def from_document(cls, document):
        return cls(**document)

    @classmethod
    async def find_one(cls, query):
        doc = await Storable._DATABASE.find_one(cls, query)
        if not doc:
            raise exceptions.NotFound()
        return cls.from_document(doc)

    @classmethod
    async def find(cls, query=None, limit=0, skip=0, sort=None):
        return (
            cls.from_document(doc)
            for doc in
            await Storable._DATABASE.find(cls, query=query, limit=limit, skip=skip, sort=sort)
        )

    @classmethod
    async def load(cls, _id):
        return cls.from_document(await Storable._DATABASE.load(cls, _id))

    def __init__(self, **kwargs):
        self._data = {
            key: value.parse(kwargs.get(key))
            for key, value in self._fields.items()
        }

    def to_document(self, translator=None):
        if translator:
            return {
                key: translator.translate_field(field, self._data.get(key))
                for key, field in self._fields.items()
            }

        return {
            key: field.to_document(self._data.get(key))
            for key, field in self._fields.items()
        }
