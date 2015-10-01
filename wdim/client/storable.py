import abc

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
                return StorableMeta[name.lower()]
            except KeyError:
                raise KeyError('Class {} not found'.format(name))
        return getter

    @classmethod
    async def _bootstrap(cls):
        for klass in Storable.__subclasses__():
            await cls._DATABASE.ensure_index(
                klass._collection_name,
                [
                    {'key': key, 'unique': value.unique}
                    for key, value in klass._fields.items()
                    if value.index
                ])
        return True

    @classmethod
    async def connect(cls, db, bootstrap=True):
        Storable._DATABASE = db
        if bootstrap:
            return await Storable._bootstrap()
        return True

    @classmethod
    async def create(cls, *, _id=None, **kwargs):
        assert _id is None, '_id cannot be supplied'
        inst = cls(**kwargs)

        _id = await cls._DATABASE.insert(inst)
        cls._fields['_id'].__set__(inst, _id, override=True)

        return inst

    @classmethod
    def from_document(cls, document):
        return cls(**document)

    @classmethod
    async def find_one(cls, query):
        return cls.from_document(
            await Storable._DATABASE.find_one(cls, query)
        )

    def __init__(self, **kwargs):
        self._data = {
            key: value.parse(kwargs.get(key))
            for key, value in self._fields.items()
        }

    async def to_document(self, join=False):
        return {
            key: value.to_document(self._data.get(key), join=join)
            for key, value
            in self._fields.items()
        }
