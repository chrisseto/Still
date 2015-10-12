import enum


class Action(enum.Enum):
    CREATE = 0
    DELETE = 1
    UPDATE = 2
    CREATE_SCHEMA = 3
    UPDATE_SCHEMA = 4
    DELETE_SCHEMA = 5
