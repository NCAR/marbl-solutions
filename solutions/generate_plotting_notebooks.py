import papermill as pm
import nbformat as nbf
from config import analysis_config

def modify_markdown_header(notebook_name, variable):
    notebook = nbf.read(notebook_name, nbf.NO_CONVERT)
    cells_to_keep = []
    for cell in notebook.cells:
        if cell.cell_type == 'markdown':
            cell['source'] = cell['source'].replace('Variable', variable)
        
        cells_to_keep.append(cell)
    new_notebook = notebook
    new_notebook.cells = cells_to_keep
    nbf.write(new_notebook, notebook_name, version=nbf.NO_CONVERT)
    return print(f'Modified {notebook_name} with {variable} header')

def nb_get_kernelname(file_in):
    """get the kernel name of a notebook"""
    data = nbf.read(file_in, as_version=nbf.NO_CONVERT)
    return data["metadata"]["kernelspec"]["name"]

for collection_type in analysis_config.keys():
    for variable in analysis_config[collection_type]['variables']:
        out_notebook_name = f"{collection_type}_{variable}.ipynb"
        pm.execute_notebook(
            analysis_config[collection_type]['template_notebook'],
            out_notebook_name,
            parameters=dict(variable=variable),
            kernel_name=nb_get_kernelname(analysis_config[collection_type]['template_notebook'])
        )
        modify_markdown_header(out_notebook_name, variable)