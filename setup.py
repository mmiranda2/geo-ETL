from setuptools import setup, find_packages

setup(
    name="geoetl",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    include_package_data=True,
    version="0.0.1",
    description="Basic ETL routines with GIS support",
    author="Michael Miranda",
    url="https://github.com/mmiranda2/geo-ETL",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent"
    ],
    install_requires=[
        "htmllistparse",
        "numpy",
        "requests",
        "s3fs",
        "xarray"
    ],
    entry_points={
        'console_scripts': [
            'extract_mrms=geoetl.examples.extract_mrms:main',
            'extract_noaa_aggregate=geoetl.examples.extract_noaa_aggregate:main',
            'test_geoetl=geoetl.tests.index:main'
        ]
    },
    python_requires=">=3.6"
)
