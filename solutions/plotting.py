import holoviews as hv
import matplotlib.pyplot as plt
import numpy as np
import panel as pn
from holoviews.operation.datashader import rasterize
from matplotlib import colors


def plot_ds_global_map(ds, variable, key, add_nlon_nlat=True, cmap='magma', vmin=None, vmax=None):
    """Plots an interactive plot using holoviews"""
    da = ds[variable]

    if add_nlon_nlat:
        da['nlon'] = ds.nlon
        da['nlat'] = ds.nlat

    # da = da.drop_vars(coords_to_drop)
    da = da.reset_coords()[[variable]]

    hvds = hv.Dataset(da, kdims=list(da.dims))

    quadmesh = hvds.to(hv.QuadMesh, kdims=['nlon', 'nlat'])
    if vmin:
        levels = list(np.linspace(vmin, vmax, 20))
        cmap = create_cmap(levels)
        datashade_obj = rasterize(quadmesh, precompute=True).opts(
            colorbar=True,
            width=600,
            height=400,
            cmap=cmap,
            tools=['hover'],
            color_levels=levels,
            bgcolor='lightgray',
        )
    else:
        datashade_obj = rasterize(quadmesh, precompute=True, cmap=plt.cm.RdBu_r).opts(
            colorbar=True, width=600, height=400, cmap=cmap, tools=['hover']
        )
    return datashade_obj.relabel(f'{key}')


def plot_collection_global_map(
    collection, variable, add_nlon_nlat=True, cmap='magma', vmin=None, vmax=None
):
    """
    Plots a collection of datasets
    """
    column = pn.Column()
    for key in collection.keys():
        column += pn.Column(
            plot_ds_global_map(
                collection[key],
                variable=variable,
                key=key,
                add_nlon_nlat=add_nlon_nlat,
                cmap=cmap,
                vmin=vmin,
                vmax=vmax,
            )
        )

    return column


def create_cmap(levs):
    assert len(levs) % 2 == 0, 'N levels must be even.'
    return colors.LinearSegmentedColormap.from_list(
        name='red_white_blue',
        colors=[(0, 0, 1), (1, 1.0, 1), (1, 0, 0)],
        N=len(levs) - 1,
    )
