import abc


class DatabaseLayer(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    async def ensure_index(self, name, keys):
        raise NotImplemented

    # Create
    @abc.abstractmethod
    async def insert(self, inst):
        raise NotImplemented

    # Read
    @abc.abstractmethod
    async def load(self, pk):
        raise NotImplemented

    @abc.abstractmethod
    async def find(self, query):
        raise NotImplemented

    @abc.abstractmethod
    async def find_one(self, query):
        raise NotImplemented

    # Update

    # Delete
    @abc.abstractmethod
    async def drop(self, pk):
        raise NotImplemented

    def __rshift__(self, other):
        # TODO resolve circular import
        from wdim.orm.database import CompoundWriteLayer
        return CompoundWriteLayer(self, other)
