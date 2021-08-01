import sys
import datetime
from io import BytesIO
from geoetl.geoapi import GDALWarpFactory
from geoetl.api import Validator, Transformer, APISource, Transform, CLIBaseTransform, UnzipTransform


class Application:
    def __init__(self, config):
        self.config = config
    
    def some_external_call(self):
        return True
    
    def is_needed(self):
        return self.some_external_call()


class SimpleValidator(Validator):
    def __init__(self, application):
        self.state = application
    
    def is_resp_valid(self, resp):
        # Integrate app as needed
        if not self.state.is_needed():
            return False
        # Enforce GZip
        if resp.headers['Content-Type'] != 'application/x-gzip':
            return False
        # Enforce <1MB
        if int(resp.headers['Content-Length']) > 10**6:
            return False
        # Enforce date in sync
        if datetime.datetime.utcnow().strftime('%d %b') not in resp.headers['Last-Modified']:
            return False

        return True


def main():
    '''
    Call NOAA api, transform to gzipped netcdf
    '''
    GDALWarpUngrib = GDALWarpFactory.make('GDALWarpUngrib')
    ZipTransform = CLIBaseTransform.make(name='ZipTransform', executable='gzip', command='gzip -c {source} > {destination}')

    simple_mrms = APISource(
        url='https://mrms.ncep.noaa.gov/data/2D/MESH/MRMS_MESH.latest.grib2.gz',
        validator=SimpleValidator(Application(None)),
        transformer=Transformer(
            transforms=[UnzipTransform, GDALWarpUngrib, ZipTransform])).transform()

    print('Output filepath: ' + simple_mrms.filepath)
    return simple_mrms


if __name__ == '__main__':
    sys.exit(main())