import sys
import s3fs
import datetime
import numpy as np
from io import BytesIO
from geoetl.geoapi import GDALWarpFactory, LoadNetcdf
from geoetl.api import Validator, Transformer, FileNode, APISource, Transform, CLIBaseTransform, UnzipTransform, ApacheIndex


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

    def transform(self) -> FileNode:
        destination_filepath = 'my_s3_folder/path/mrms.nc.gz'
        bucket.put(self.source_filepath, destination_filepath)
        return FileNode(filepath=destination_filepath)


class NOAAValidator(Validator):
    def __init__(self):
        self.previous = []

    def is_valid(self, listing) -> bool:
        # Enforce <1MB
        if int(listing.size) > 2*10**6:
            print('oversize')
            return False
        # Enforce date in sync
        dt = datetime.datetime.utcnow()
        month, day = dt.month, dt.day
        if month != listing.modified.tm_mon and day != listing.modified.tm_mday:
            print('not today')
            return False
        return True


def noaa_aggregator(a: FileNode, b: FileNode) -> FileNode:
    return FileNode(in_memory=True, content=np.maximum(a.content, b.content))


GDALWarpUngrib = GDALWarpFactory.make('GDALWarpUngrib')
ZipTransform = CLIBaseTransform.make(name='ZipTransform', executable='gzip', command='gzip -c {source} > {destination}')


def main():
    aggregated_node = ApacheIndex(
        url='https://mrms.ncep.noaa.gov/data/2D/MESH',
        validator=NOAAValidator(),
        transformer=Transformer(
            transforms=[UnzipTransform, GDALWarpUngrib, LoadNetcdf])).get_aggregate(noaa_aggregator)
    
    print('Output filepath: ' + aggregated_node.filepath)


if __name__ == '__main__':
    sys.exit(main())
