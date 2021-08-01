import sys
import s3fs
import datetime
import numpy as np
from io import BytesIO
from geoetl.geoapi import GDALWarpFactory, LoadNetcdf, xarray_sum
from geoetl.api import Validator, Transformer, FileNode, APISource, Transform, CLIBaseTransform, UnzipTransform, ApacheIndex


class NOAAValidator(Validator):
    def __init__(self):
        self.date = datetime.datetime.utcnow()

    def is_valid(self, listing) -> bool:
        # Enforce <1MB
        if int(listing.size) > 2*10**6:
            print('oversize')
            return False
        
        # Enforce date in sync
        month, day = self.date.month, self.date.day
        if month != listing.modified.tm_mon and day != listing.modified.tm_mday:
            print('Not today')
            return False
        return True


def main():
    '''
    Sum all netCDFs on the Apache server into one netCDF
    If one arg is given to get_aggregate, it will be called on the list of nodes, i.e. xarray_aggregator(nodes)
    If given two args, the first will be called as the reducer and second fed into the reducer
    i.e. reduce(noaa_aggregator, list)
    '''

    GDALWarpUngrib = GDALWarpFactory.make('GDALWarpUngrib')
    ZipTransform = CLIBaseTransform.make(name='ZipTransform', executable='gzip', command='gzip -c {source} > {destination}')

    aggregated_node = ApacheIndex(
        url='https://mrms.ncep.noaa.gov/data/2D/MESH',
        validator=NOAAValidator(),
        transformer=Transformer(
            transforms=[UnzipTransform, GDALWarpUngrib, LoadNetcdf])).get_aggregate(xarray_sum)
    
    print('Output filepath: ' + aggregated_node.filepath)


if __name__ == '__main__':
    sys.exit(main())
