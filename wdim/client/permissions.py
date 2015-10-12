import sys
import enum
import functools


class Permissions(enum.IntEnum):
    NONE = 0x0

    CREATE = 0x1
    READ = 0x1 << 1
    UPDATE = 0x1 << 2
    DELETE = 0x1 << 3

    READ_WRITE = CREATE | READ | UPDATE
    CRUD = READ_WRITE | DELETE
    ADMIN = sys.maxsize  # Grants all possible permission

    @classmethod
    def from_method(cls, http_method):
        return {
            'post': Permissions.CREATE,
            'get': Permissions.READ,
            'put': Permissions.UPDATE,
            'patch': Permissions.UPDATE,
            'delete': Permissions.DELETE,
        }[http_method.lower()]

    @classmethod
    def get_permissions(cls, user, *perm_objs):
        user = user or ''
        provider = user.split('-')[0]

        return functools.reduce(
            lambda acc, perm: (acc
                | perm.get('*', Permissions.NONE)
                | perm.get(user, Permissions.NONE)
                | perm.get(provider, Permissions.NONE)),
            (obj.permissions for obj in perm_objs),
            Permissions.NONE
        )
