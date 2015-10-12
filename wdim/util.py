import functools


def pack(*args, **kwargs):
    return (args, kwargs)


def merge(target, source, path=None, update=True):
    # http://stackoverflow.com/questions/7204805/python-dictionaries-of-dictionaries-merge
    path = path or []
    for key in source:
        if key in target:
            if isinstance(target[key], dict) and isinstance(source[key], dict):
                merge(target[key], source[key], path + [str(key)])
            elif target[key] == source[key]:
                pass  # same leaf value
            elif isinstance(target[key], list) and isinstance(source[key], list):
                for idx, val in enumerate(source[key]):
                    target[key][idx] = merge(target[key][idx], source[key][idx], path + [str(key), str(idx)], update=update)
            elif update:
                target[key] = source[key]
            else:
                raise Exception('Conflict at %s' % '.'.join(path + [str(key)]))
        else:
            target[key] = source[key]
    return target


def combomethod(failback=False):
    class _combomethod(object):
        def __init__(self, method):
            self.method = method

        def __get__(self, inst, klass):
            @functools.wraps(self.method)
            def _wrapper(*args, **kwargs):
                if inst:
                    return self.method(inst, *args, **kwargs)
                try:
                    return getattr(super(klass, klass), self.method.__name__)(*args, **kwargs)
                except AttributeError:
                    if not failback:
                        raise
                    return self.method(klass, *args, **kwargs)
            return _wrapper

    if callable(failback):
        method, failback = failback, False
        return _combomethod(method)
    return _combomethod
