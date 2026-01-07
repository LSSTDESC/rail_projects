from typing import Any

import numpy as np
from rail.utils.path_utils import find_rail_file


def make_band_names(template: str, bands: list[str]) -> list[str]:
    """Make a set of band names from template and a list of bands

    Parameters
    ----------
    template:
        Template to make the names

    bands:
        List of the bands to apply to the template

    Returns
    -------
    Names of the bands
    """
    return [template.format(band=band_) for band_ in bands]


def extract_data_to_2d_array(data: Any, column_names: list[str]) -> np.ndarray:
    """Extract a set of columns from a table to a 2D array

    Parameters
    ----------
    data:
        Input data

    column_names:
        Names of the columns to extract

    Returns
    -------
    Output 2D-Array
    """
    column_data = [data[column_] for column_ in column_names]
    return np.vstack(column_data).T


def get_band_values(
    input_data: np.ndarray,
    band_name_template: str,
    bands: list[str],
) -> np.ndarray:
    """Extract a set of columns from a table to a 2D array

    Parameters
    ----------
    input_data:
        Input data

    template:
        Template to make the names

    bands:
        List of the bands to apply to the template

    Returns
    -------
    Output 2D-Array
    """
    band_names = make_band_names(band_name_template, bands)
    values = extract_data_to_2d_array(input_data, band_names)
    return values


def fluxes_to_mags(
    flux: np.ndarray,
    zero_point: float = 31.4,
) -> np.ndarray:
    """Convert flux values to AB magnitudes.

    This function converts flux measurements to AB magnitudes using the
    standard astronomical magnitude formula: m = -2.5 * log10(flux) + zero_point.
    Negative or zero flux values result in NaN magnitudes.

    Parameters
    ----------
    flux
        Array of flux values. Must be non-negative for valid magnitude
        calculations. Values <= 0 will produce NaN in the output.
    zero_point
        Zero-point offset to apply to the magnitude calculation. This accounts
        for the flux scale and instrumental calibration. Default is 31.4.

    Returns
    -------
        Array of AB magnitudes with the same shape as the input flux array.
        Elements corresponding to flux <= 0 will be NaN.
    """
    # Handle negative and zero fluxes by replacing with NaN
    with np.errstate(divide="ignore", invalid="ignore"):
        mags = -2.5 * np.log10(flux) + zero_point

    return mags


def adjacent_band_colors(mags: np.ndarray) -> np.ndarray:
    """Return a set of colors using magnitudes in adjacent bands

    I.e., u-g, g-r, r-i, i-z, z-y

    Note that there will be one less color than bands

    Parameters
    ----------
    mags:
        Input data

    Returns
    -------
    Output colors
    """
    n_bands = mags.shape[-1]
    colors = [mags[:, i] - mags[:, i + 1] for i in range(n_bands - 1)]
    return np.vstack(colors).T


def build_template_dict(
    seds: list[str], filters: list[str]
) -> dict[str, tuple[np.ndarray, np.ndarray, np.ndarray, int]]:  # pragma: no cover
    """Extract AB templates

    Parameters
    ----------
    seds:
        Names of the seds to use

    filters:
        Name of the filters to use

    Returns
    -------
    Dict mapping sed name to a tuple with
    redshifts (N), mags (N, N_filter), colors (N, N_filter-1), sed_index
    """
    template_dict = {}
    for ised, sed in enumerate(seds):
        mag_data_list = []
        for filter_ in filters:
            path = find_rail_file(
                f"examples_data/estimation_data/data/AB/{sed}.{filter_}.AB"
            )
            data = np.loadtxt(path)
            _redshifts = data[:, 0]
            mags = fluxes_to_mags(data[:, 1], 31.4)
            mag_data_list.append(mags)
        mag_data = np.vstack(mag_data_list).T
        color_data = adjacent_band_colors(mag_data).T
        template_dict[sed] = (_redshifts, mag_data, color_data, ised)
    return template_dict
