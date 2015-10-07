import abc


class Sorting(abc.ABC):

    # __slots__ = ('_field', )

    @property
    def field(self):
        return self._field

    def __init__(self, field):
        self._field = field


class Descending(Sorting):
    pass


class Ascending(Sorting):
    pass
