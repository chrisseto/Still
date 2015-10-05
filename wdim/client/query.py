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
    def querys(self):
        return self._querys

    def __init__(self, *querys):
        self._querys = querys

    def __iter__(self):
        return self.querys

    def __repr__(self):
        return '<{}({})>'.format(
            self.__class__.__name__,
            ', '.join([q.__repr__() for q in self.querys])
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

    def __init__(self, name, value):
        self._name = name
        self._value = value

    def __repr__(self):
        return '<{}({}, {})>'.format(self.__class__.__name__, self.name, self.value)

    def __str__(self):
        return self.__repr__()


class Equals(Query):
    pass


class And(JointQuery):
    pass


class Or(JointQuery):
    pass