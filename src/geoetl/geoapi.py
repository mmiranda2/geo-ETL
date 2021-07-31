import os
import json
import numpy as np
import xarray as xr
from typing import List, Dict, Type
from .api import Transform, FileNode, CLIBaseTransform
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


class LoadNetcdf(Transform):
    def __init__(self, *args, **kwargs):
        super(LoadNetcdf, self).__init__(*args, **kwargs)
    
    def transform(self):
        source_nc = xr.open_dataset(self.source_filepath, engine='netcdf4')
        source_np = np.array(source_nc.Band1.values[:,:])
        source_np[source_np < 0] = 0
        source_np = source_np.astype(np.uint8)
        return FileNode(in_memory=True, content=source_np)