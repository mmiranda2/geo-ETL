import os
import requests
import htmllistparse
from io import BytesIO
from functools import reduce
from typing import List, Dict, Type
from collections.abc import Callable
from .utils import xor, get_temp_file


class Validator:
    '''
    Handle application state here. Allows is_valid to make stateful decisions, e.g. caching
    '''
    def __init__(self):
        pass
        
    def is_valid(self):
        pass


class FileNode:
    '''
    FileNode objects are connected by transforms.
    Applying T to a FileNode returns a FileNode.
    in_memory here is a descriptor, not an instruction
    '''
    def __init__(self, filepath: str='', metadata: dict={}, in_memory: bool=False, content=BytesIO()):
        if not xor(filepath, in_memory):
            raise Exception('Must provide a filepath or enable in_memory')

        self.filepath = filepath
        self.metadata = metadata
        self.in_memory = in_memory
        self.content = content
    
    def truncate(self) -> None:
        if self.in_memory:
            try:
                self.content.truncate(0)
                self.content.seek(0)
                self.content = None
            except:
                del self.content
                self.content = None
        else:
            if os.path.exists(self.filepath):
                os.remove(self.filepath)


class Transform:
    '''
    A directed edge between two FileNode objects, connected by .transform()
    '''
    def __init__(self, node: FileNode=None, debug=False):
        self.source_node = node
        self.source_filepath = node.filepath
        self.source_metadata = node.metadata
        self.source_in_memory = node.in_memory
        self.source_content = node.content
        self.debug = debug

    def transform(self):
        self.finalize()

    def finalize(self):
        pass
    
    def get_temp_file(self) -> str:
        return get_temp_file(prefix=self.__name__)
    
    def remove_source(self) -> None:
        self.source_node.truncate()


class CLIBaseTransform(Transform):

    executable = ''
    command = ''

    def __init__(self, *args, **kwargs):
        super(CLIBaseTransform, self).__init__(*args, **kwargs)

    def _format_command(self, source_filepath: str, destination_filepath: str) -> str:
        return self.command.replace('{source}', source_filepath).replace('{destination}', destination_filepath)

    def execute(self, source: str, dest: str) -> int:
        cmd = os.system(self._format_command(source, dest))
        exit_code = os.WEXITSTATUS(cmd)
        return exit_code

    def transform(self) -> FileNode:
        destination_filepath = self.get_temp_file()
        self.execute(self.source_filepath, destination_filepath)
        return FileNode(filepath=destination_filepath)
    
    @classmethod
    def make(cls, name, executable, command):
        return type(
            name,
            (cls,),
            {'executable': executable, 'command': command})

    @classmethod
    def check_installation() -> bool:
        cmd = os.system(f'command -v {cls.executable} > /dev/null 2>&1')
        exit_code = os.WEXITSTATUS(cmd)
        if exit_code:
            print(f'Command {cls.executable} not found, please install and put in PATH')
            return False
        return True


class UnzipTransform(CLIBaseTransform):
    executable = 'gunzip'
    command = 'gunzip -c {source} > {destination}'

    def __init__(self, *args, **kwargs):
        super(UnzipTransform, self).__init__(*args, **kwargs)


class Transformer:
    def __init__(self, transforms: List[Type[Transform]], remove_source=True, debug=False):
        self.transforms = transforms
        self.remove_source = remove_source
        self.debug = debug

    def transformed_node(self, root: FileNode) -> FileNode:
        return reduce(lambda node, T: self.apply(node, T), [root] + self.transforms)
     
    def apply(self, node, T) -> FileNode:
        print('Applying ' + T.__name__)
        initial = T(node, debug=self.debug)                   # Transform object
        output_file_node = initial.transform()          # FileNode object
        if self.remove_source:
            initial.remove_source()
        initial.finalize()

        return output_file_node


class APISource:
    def __init__(self, url: str, validator: Validator, transformer: Transformer, name: str=''):
        self.url = url
        self.validator = validator
        self.transformer = transformer
        self.name = name
    
    def download(self) -> FileNode:
        resp = requests.get(self.url)
        is_valid = self.validator.is_resp_valid(resp)

        if resp.status_code != 200:
            raise Exception('Failed request for ' + (name or self.url))
        
        filepath = get_temp_file()
        with open(filepath, 'wb') as f:
            f.write(resp.content)
        
        return FileNode(filepath=filepath, metadata={'is_valid': is_valid})
    
    def transform(self) -> FileNode:
        return self.transformer.transformed_node(root=self.download())
   

class Listing:
    def __init__(self, url: str):
        self.url = url
    
    def download(self) -> FileNode:
        resp = requests.get(self.url)
        if resp.status_code != 200:
            raise Exception('Failed request for ' + listing.name)

        filepath = get_temp_file()
        with open(filepath, 'wb') as f:
            f.write(resp.content)

        return FileNode(filepath=filepath)


class Index:
    def __init__(self, url: str, validator: Validator, transformer: Transformer):
        # index url, e.g. apache folder
        self.url = url
        self.validator = validator
        self.transformer = transformer
        self.listings = []
    
    def fetch_listings(self):
        '''
        Extend this to grab file links from a source, e.g. object names in S3
        '''
        raise NotImplementedError

    def aggregate(self, aggregator, *args, **kwargs) -> FileNode:
        nodes = self.transform_index()
        result = aggregator(*args, nodes=nodes, **kwargs)

        return result
    
    def transform_node(self, listing: Listing) -> FileNode:
        return self.transformer.transformed_node(root=self.download_listing(listing))
    
    def transform_index(self) -> List[FileNode]:
        nodes = map(lambda listing: self.transform_node(listing), filter(self.validator.is_valid, self.listings))
        return nodes


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