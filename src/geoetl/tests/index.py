import sys
from geoetl.utils import timeit
from .test_gdalwarp import main as test_gdalwarp
from .test_etl_chain import main as test_etl_chain


def main():
    tests = test_gdalwarp + test_etl_chain
    
    results, t = timeit(map(lambda test: test(), tests))
    num_passed = len([r for r in results if r])

    print(f'{num_passed} out of {len(results)} tests passed.')


if __name__ == '__main__':
    sys.exit(main())