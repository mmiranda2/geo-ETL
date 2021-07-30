import tempfile


def xor(a, b):
    return bool(a) ^ bool(b)


def get_temp_file():
    with tempfile.NamedTemporaryFile(delete=False) as f:
        return f.name