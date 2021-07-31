import tempfile


def xor(a, b):
    return bool(a) ^ bool(b)


def get_temp_file():
    try:
        with tempfile.NamedTemporaryFile(delete=False, dir='/factory') as f:
            return f.name
    except:
        with tempfile.NamedTemporaryFile(delete=False, dir='./') as f:
            return f.name