"""
Fetch metadata for testing. Actual data values are not downloaded, keeping the file
size reasonable.
"""
import pathlib

import adlfs
import numpy as np
import planetary_computer
import xarray as xr

ROOT = pathlib.Path(__file__).parent.parent
DATA = ROOT / "tests/data"


def main():

    account_name = "ukmeteuwest"
    container_name = "ukcp18"
    credential = planetary_computer.sas.get_token(account_name, container_name).token

    fs = adlfs.AzureBlobFileSystem("ukmeteuwest", credential=credential)
    account_name = "ukmeteuwest"
    container_name = "ukcp18"
    credential = planetary_computer.sas.get_token(account_name, container_name).token
    fs = adlfs.AzureBlobFileSystem(account_name, container_name, credential=credential)
    paths = fs.glob(
        "ukcp18/badc/ukcp18/data/land-gcm/global/60km/rcp26/01/*/day/*/*18991201-19091130.nc"
    )
    datasets = [xr.open_dataset(fs.open(path), chunks={}) for path in paths]

    for dataset in datasets:
        # These are stored as size-1 object-dtype ndarrays
        for k in ["label_units", "plot_label"]:
            if (
                isinstance(dataset.attrs[k], np.ndarray)
                and dataset.attrs[k].dtype == "object"
            ):
                dataset.attrs[k] = dataset.attrs[k].astype(str)

    filenames = []
    for path, dataset in zip(paths, datasets):
        filename = pathlib.Path("/tmp/") / pathlib.Path(path).name
        dataset.to_netcdf(str(filename), compute=False)
        assert filename.exists()
        filenames.append(filename)


if __name__ == "__main__":
    main()
