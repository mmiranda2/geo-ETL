import s3fs
import datetime
from io import BytesIO
from geoapi import GDALWarpFactory
from api import Validator, Transformer, APISource, Transform, CLIBaseTransform, UnzipTransform


class SimpleS3Bucket:
    def __init__(self, bucket):
        self.s3 = s3fs.S3FileSystem(anon=False)
        self.bucket = bucket
        if self.bucket[-1] != '/':
            self.bucket += '/'
    
    def get(self, source_filepath, destination_filepath):
        with self.s3.open(bucket + source_filepath, 'rb') as s:
            with open(destination_filepath, 'wb') as f:
                f.write(s.read())

    def get_obj(self, source_filepath):
        with self.s3.open(bucket + source_filepath, 'rb') as s:
            return BytesIO(s.read())

    def put(self, source_filepath, destination_filepath):
        with self.s3.open(bucket + destination_filepath, 'wb') as f:
            with open(source_filepath, 'rb') as s:
                f.write(s.read())
    
    def put_obj(self, source_content, destination_filepath):
        with self.s3.open(bucket + destination_filepath, 'wb') as f:
            f.write(source_content.getvalue())


class S3Transform(Transform):

    bucket = None # SimpleS3Bucket() 

    def transform(self):
        destination_filepath = 'my_s3_folder/path/mrms.nc.gz'
        bucket.put(self.source_filepath, destination_filepath)
        return FileNode(filepath=destination_filepath)
    
    def finalize(self):
        if os.path.exists(self.source_filepath):
            os.remove(self.source_filepath)


class SimpleValidator(Validator):
    def __init__(self):
        self.previous = []

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


if __name__ == '__main__':
    simple_mrms = APISource(
        url='https://mrms.ncep.noaa.gov/data/2D/MESH/MRMS_MESH.latest.grib2.gz',
        validator=SimpleValidator(),
        transformer=Transformer(
            transforms=[UnzipTransform, GDALWarpUngrib, ZipTransform])).transform()