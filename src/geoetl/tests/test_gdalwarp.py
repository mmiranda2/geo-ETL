from io import BytesIO
from geoetl.geoapi import GDALWarpFactory
from geoetl.utils import gdalinfo, test_decorator
from geoetl.api import FileNode, Transformer, UnzipTransform
from geoetl.examples.extract_mrms import main as extract_mrms


ZipTransform = CLIBaseTransform.make(name='ZipTransform', executable='gzip', command='gzip -c {source} > {destination}')


@test_decorator
def is_gdalwarp_installed():
    return GDALWarpFactory.check_installation()


@test_decorator
def is_gdalwarp_netcdf():
    transformer = Transformer(transforms=[UnzipTransform])
    result_nc_node = transformer.transformed_node(root=extract_mrms())
    result_gdalinfo = gdalinfo(result_nc_node.filepath)
    
    return 'netcdf' in result_gdalinfo.get('driverShortName', '')


def main():
    return [is_gdalwarp_installed, is_gdalwarp_netcdf]