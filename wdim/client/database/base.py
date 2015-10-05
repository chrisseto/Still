import abc


class DatabaseLayer(metaclass=abc.ABCMeta):

    @classmethod
    @abc.abstractmethod
    def translate_query(self, query):
        pass

    @classmethod
    @abc.abstractmethod
    def translate_sorting(self, query):
        pass

    @abc.abstractmethod
    async def insert(self, inst):
        raise NotImplemented

    @abc.abstractmethod
    async def load(self, pk):
        raise NotImplemented

    @abc.abstractmethod
    async def ensure_index(self, name, keys):
        raise NotImplemented
