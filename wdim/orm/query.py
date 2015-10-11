import abc


class BaseQuery(abc.ABC):

    def __and__(self, other):
        return And(self, other)

    def __or__(self, other):
        return Or(self, other)

    def __add__(self, other):
        return self & other


class JointQuery(BaseQuery, metaclass=abc.ABCMeta):

    @property
    def queries(self):
        return self._queries

    def __init__(self, *queries):
        self._queries = queries

    def __iter__(self):
        return self.queries

    def __repr__(self):
        return '<{}({})>'.format(
            self.__class__.__name__,
            ', '.join([q.__repr__() for q in self.queries])
        )

    def __str__(self):
        return self.__repr__()


class Query(BaseQuery, metaclass=abc.ABCMeta):

    @property
    def name(self):
        return self._name

    @property
    def value(self):
        return self._value

    def __init__(self, field, value):
        self._field = field
        self._name = field._name
        self._value = field.parse(value)

    def __repr__(self):
        return '<{}({}, {!r})>'.format(self.__class__.__name__, self.name, self.value)

    def __str__(self):
        return self.__repr__()


class Equals(Query):
    pass


class And(JointQuery):
    pass


class Or(JointQuery):
    pass
