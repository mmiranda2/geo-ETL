import sys
from .test_etl_chain import test_zip_transformer


def main():
    test_zip_transformer()


if __name__ == '__main__':
    sys.exit(main())