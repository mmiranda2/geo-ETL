import os
import json
import htmllistparse
import numpy as np
import xarray as xr
from typing import List, Dict, Type
from .api import Transform, FileNode, CLIBaseTransform, Listing, Index
from .utils import get_temp_file


def gdalinfo(filepath: str) -> Dict:
    dest_fp = get_temp_file()
    os.system(f'gdalinfo -json {filepath} > {dest_fp}')
    with open(dest_fp, 'r') as f:
        file_gdalinfo = json.load(f)
    os.remove(dest_fp)
    return file_gdalinfo


class GDALWarpBase(CLIBaseTransform):

    executable = 'gdalwarp'

    def __init__(self, *args, **kwargs):
        super(GDALWarpBase, self).__init__(*args, **kwargs)


class GDALWarpFactory:

    _process_mapping = {
        'GDALWarpUngrib': ('gdalwarp_grib_to_nc', 'gdalwarp -t_srs EPSG:4326 -ot Int16 -of netCDF {source} {destination}')
    }

    @classmethod
    def search_process(cls, name) -> str:
        process = name
        if name not in cls._process_mapping:
            keys = [k for k, v in cls._process_mapping.items() if name == v.__name__]
            if keys:
                process = keys[0]
            else:
                return ''
        return process
    
    @classmethod
    def make(cls, name: str) -> Type[GDALWarpBase]:
        # Find out what "name" refers to
        process = cls.search_process(name)
        if not process:
            raise Exception('Process/Transform not found')
        
        return type(
            process, 
            (GDALWarpBase,),
            {'command': cls._process_mapping[process][1]})

    @classmethod
    def list_all_funcs(cls) -> Dict[str, str]:
        return {k: v.__name__ for k, v in cls._process_mapping.items()}
    
    @staticmethod
    def check_installation() -> bool:
        cmd = os.system(f'gdalwarp --version')
        exit_code = os.WEXITSTATUS(cmd)
        if exit_code:
            print(f'Gdalwarp binary not found, please install and put in PATH')
            return False
        return True


class NetcdfTransform(Transform):
    '''
    For netCDF -> netCDF and netCDF -> X operations
    '''
    def __init__(self, *args, **kwargs):
        super(LoadNetcdf, self).__init__(*args, **kwargs)
        self.file_gdalinfo = gdalinfo(self.source_filepath)
        try:
            self.bands = [band['metadata']['']['NETCDF_VARNAME'] for band in self.file_gdalinfo['bands']]
        except:
            self.bands = ['Band1']
        if not self.bands:
            raise ValueError('No NetCDF bands found')
    
    def transform(self):
        band = self.source_node.metadata.get('band', 'Band1')
        source_xr = self.get_xr_dataset(self.source_filepath)
        for band in self.bands:
            band_ref = get_band_ref(source_xr, band)
            band_ref.values = self.get_absolute(band_ref)
        
        destination_filepath = self.get_temp_file()
        source_xr.to_netcdf(destination_filepath)
        return FileNode(filepath=destination_filepath)
    
    @staticmethod
    def get_absolute(band_ref):
        source_np = np.array(band_ref.values[:,:])
        source_np[source_np < 0] = 0
        source_np = source_np.astype(np.uint8)
        return source_np

    @staticmethod
    def get_band_ref(xr_dataset, band='Band1'):
        return getattr(xr_dataset, band)
    
    @staticmethod
    def get_xr_dataset(source_filepath, chunks=None):
        return xr.open_dataset(source_filepath, engine='netcdf4', chunks=chunks)
    
    @staticmethod
    def get_xr_mfdataset(source_filepaths, chunks=None):
        # Stack xr datasets along new dimension 't'
        return xr.open_mfdataset(
            source_filepaths,
            engine='netcdf4', concat_dim='t', combine='nested', chunks=8)


class ApacheListing(Listing):
    def __init__(self, listing, base_url: str):
        self.name = listing.name
        self.modified = listing.modified
        self.size = listing.size
        self.description = listing.description
        self.base_url = base_url

        if base_url[-1] != '/':
            base_url += '/'
        self.url = base_url + self.name

    @classmethod
    def format(cls, listings, url: str) -> List[Listing]:
        return [cls(listing=listing, base_url=url) for listing in listings]


class ApacheIndex(Index):
    '''
    ETL Connector for NOAA Apache servers
    '''
    def fetch_listings(self):
        cwd, listings = htmllistparse.fetch_listing(self.url, timeout=30)
        self.cwd = cwd
        self.listings = ApacheListing.format(listings, self.url)

    def get_aggregate(self, aggregator, *args, **kwargs) -> FileNode:
        self.fetch_listings()
        return self.aggregate(aggregator, *args, **kwargs)


def xr_mf_sum(nodes: List[FileNode]) -> FileNode:
    '''
    Return sum of "band" over all netCDF FileNodes 
    '''
    band = nodes[0].metadata.get('band', 'Band1')
    
    # Open xarray multi-file dataset and open latest node
    mf_xr = NetcdfTransform.get_xr_mfdataset([node.filepath for node in nodes])
    latest_xr = NetcdfTransform.get_xr_dataset(nodes[-1].filepath)
    latest_band_ref = NetcdfTransform.get_band_ref(latest_xr, band)

    # Insert summed result of "band" into latest xr
    sum_band_ref = NetcdfTransform.get_band_ref(mf_xr.sum('z'), band)
    latest_band_ref.values = NetcdfTransform.get_absolute(sum_band_ref)

    destination_filepath = get_temp_file()
    latest_xr.to_netcdf(destination_filepath)
    return FileNode(filepath=destination_filepath)


def xarray_sum(nodes, mode='dim'):
    if mode == 'dim':
        return xr_mf_sum(nodes)


