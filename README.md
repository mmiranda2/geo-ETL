# geoETL
Basic ETL tools with support for geographic data. Made to incorporate GIS processes into standard ETL routines.

### Usage
- Make sure docker is installed
- ./build.sh to put src into a ~2.5Gb Ubuntu image with python, GDAL, and most drivers installed
- ./run.sh to open a shell in a new container
- Within shell, run console commands as needed

### TODO
- Demo: automatic detection-extraction-aggregation tool for any NOAA product (parameter: any valid NOAA Apache server)
- Some GeoAPI functions, basic GDAL and NetCDF tools
- API tools for database and S3 usage
- GeoAPI, more formats and GDAL tools, GeoJSON utilities
- Ability to parallelize tasks
