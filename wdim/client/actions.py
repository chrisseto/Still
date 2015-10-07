import enum


class Action(enum.Enum):
    create = 0
    delete = 1
    update = 2
    create_schema = 3
    update_schema = 4
    delete_schema = 5
