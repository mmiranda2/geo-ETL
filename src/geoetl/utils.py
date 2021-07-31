import tempfile
import time
from functools import wraps


def xor(a, b):
    return bool(a) ^ bool(b)


def get_temp_file():
    try:
        with tempfile.NamedTemporaryFile(delete=False, dir='/factory') as f:
            return f.name
    except:
        with tempfile.NamedTemporaryFile(delete=False, dir='./') as f:
            return f.name


def timeit(f, *args, **kwargs):
    s = time.time()
    output = f(*args, **kwargs)
    e = time.time()
    return output, e - s


def test_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        is_correct, t = timeit(func, *args, **kwargs)

        print(f'{func.__name__}? {is_correct} :: {t}')
        return is_correct

    return wrapper
