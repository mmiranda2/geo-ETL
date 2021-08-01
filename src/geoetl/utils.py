import tempfile
import time
from functools import wraps


def xor(a, b):
    return bool(a) ^ bool(b)


def get_temp_file(prefix=''):
    etl_dir = os.environ.get('ETL_DIR', './')
    with tempfile.NamedTemporaryFile(delete=False, dir=etl_dir) as f:
        folder = '/'.join(f.name.split('/')[:-1])
        filename = f.name.split('/')[-1]
        return f'{folder}/{prefix}_{filename}'


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
