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
        # Enforce GZip
        if resp.headers['Content-Type'] != 'application/x-gzip':
            return False
        # Enforce <1MB
        if int(resp.headers['Content-Length']) > 10**6:
            return False
        # Enforce fresh file
        if resp.headers['Last-Modified'] == self.previous:
            return False
        # Enforce date in sync
        if datetime.datetime.utcnow().strftime('%d %b') not in resp.headers['Last-Modified']:
            return False

        self.previous.append(resp.headers['Last-Modified'])
        return True


GDALWarpUngrib = GDALWarpFactory.make('GDALWarpUngrib')
ZipTransform = CLIBaseTransform.make(name='ZipTransform', executable='gzip', command='gzip -c {source} > {destination}')


def main():
    simple_mrms = APISource(
        url='https://mrms.ncep.noaa.gov/data/2D/MESH/MRMS_MESH.latest.grib2.gz',
        validator=SimpleValidator(),
        transformer=Transformer(
            transforms=[UnzipTransform, GDALWarpUngrib, ZipTransform])).transform()

    print('Output filepath: ' + simple_mrms.filepath)


if __name__ == '__main__':
    sys.exit(main())