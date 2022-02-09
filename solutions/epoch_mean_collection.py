import ast
import os
import tempfile

import intake
import xcollection as xc
from prefect import Flow, Parameter, task
from xpersist import CacheStore, XpersistResult

os.environ['PREFECT__FLOWS__CHECKPOINTING'] = 'True'

store = CacheStore(f'{tempfile.gettempdir()}/marbl-example-cache')


def epoch_mean(ds):
    """Take the average over some time dimension"""
    return ds.mean(dim='time')


def convert_to_collection(keys, dsets):
    """Converts keys and datasets to an xcollection.Collection"""
    return xc.Collection({keys: dsets for keys, dsets in zip(keys, dsets)})


@task
def read_catalog(path, multivar_row=False):
    """
    Reads in an intake-esm catalog from a given path

    Parameters
    ----------
    path: str, path to the associated intake-esm catalog json

    multivar_row: boolean, default=False, whether multiple variables are in each row

    Returns
    -------
    intake_esm.Catalog
    """
    if multivar_row:
        read_csv_kwargs = {'converters': {'variables': ast.literal_eval}}
    else:
        read_csv_kwargs = None

    return intake.open_esm_datastore(path, read_csv_kwargs=read_csv_kwargs)


@task
def subset_catalog(intake_esm_catalog, search_dict):
    """
    Subsets an intake-esm catalog given a search dictionary

    Parameters
    ----------
    intake_esm_catalog: intake_esm.Catalog

    search_dict: dict, dictionary to use for searching the catalog

    Returns
    -------
    catalog_subset: intake_esm.Catalog, catalog subsetted for given search
    """
    return intake_esm_catalog.search(**search_dict)


@task
def load_catalog(intake_esm_catalog, cdf_kwargs={}):
    """
    Loads the intake-esm catalog datasets into an xcollection.Collection

    Parameters
    ----------
    intake_esm_catalog: intake_esm.Catalog, catalog to read the datasets from

    cdf_kwargs: dict, dataset kwargs for intake-esm (cdf_kwargs)

    Returns
    -------
    collection: xcollection.Collection
    """
    return intake_esm_catalog.to_collection(cdf_kwargs=cdf_kwargs)


@task
def get_task_attr(obj, attr):
    """
    Retrives an attribute from a Prefect.Task

    Parameters
    ----------
    obj: Prefect.Task, the task you would like to retrieve the attributes from

    attr: str, attribute you would like to retrieve

    Returns
    -------
    object_out: Desired attribute
    """
    return list(getattr(obj, attr)())


@task(
    target='{task_name}-{key}.zarr',
    result=XpersistResult(store, serializer='xarray.zarr', serializer_dump_kwargs={'mode': 'w'}),
)
def epoch_average(ds, key):
    return epoch_mean(ds)


with Flow('epoch_mean') as epoch_mean_flow:
    catalog_path = Parameter(
        'catalog_path',
        default='/glade/work/mgrover/cesm2_le_test_data/catalogs/cesm2-le-subset.json',
    )
    multivar_row = Parameter('multi_var_row', default=False)
    search_dict = Parameter('search_dict', default={})
    cdf_kwargs = Parameter('cdf_kwargs', default=None)

    # Load the intake-esm catalog into memory
    data_catalog = read_catalog(catalog_path, multivar_row)

    # Subset the catalog
    data_catalog_subset = subset_catalog(data_catalog, search_dict)

    # Convert to catalog to an xcollection
    collection = load_catalog(data_catalog_subset, cdf_kwargs=cdf_kwargs)

    values = get_task_attr(collection, 'values')
    keys = get_task_attr(collection, 'keys')

    # Apply the epoch average operator
    # Build a prefect flow wrapper - runs flow, returns collection
    epoch_average_collection = epoch_average.map(values, keys)


def epoch_mean_flow_collection(catalog_path, multi_var_row=False, search_dict={}, cdf_kwargs=None):
    """
    Parameters
    ----------
    collection_path = str, path to intake-esm json
    multi_var_row = bool, True for history files, False for timeseries files
    search_dict = dict, search dictionary
    cdf_kwargs = dict, open dataset arguments
    """
    run_flow = epoch_mean_flow.run(
        catalog_path=catalog_path,
        multi_var_row=multi_var_row,
        search_dict=search_dict,
        cdf_kwargs=cdf_kwargs,
    )

    return convert_to_collection(
        run_flow.result[keys].result, run_flow.result[epoch_average_collection].result
    )
