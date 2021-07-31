from io import BytesIO
from geoetl.api import FileNode, Transformer, Transform, CLIBaseTransform, UnzipTransform


ZipTransform = CLIBaseTransform.make(name='ZipTransform', executable='gzip', command='gzip -c {source} > {destination}')


def is_zip_transformer_correct():
    transformer = Transformer(transforms=[ZipTransform, UnzipTransform])
    initial_value = b'asdfASDF123456789!@#$%^&*();'
    source_filepath = './test_source'

    # Ensure BytesIO -> open -> transformer -> node is correct
    bio = BytesIO(initial_value)
    with open(source_filepath, 'wb') as f:
        f.write(bio.getvalue())
    
    initial_node = FileNode(filepath=source_filepath)
    output_node = transformer.transformed_node(root=initial_node)
    with open(output_node.filepath, 'rb') as f:
        output_value = f.read()
    
    output_node.truncate()
    
    return initial_value == output_value


def test_zip_transformer():
    assert is_zip_transformer_correct()
