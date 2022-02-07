"""
Fetch metadata for testing. Actual data values are not downloaded, keeping the file
size reasonable.
"""
import pathlib

import fsspec
import planetary_computer
import xarray as xr


ROOT = pathlib.Path(__file__).parent.parent
DATA = ROOT / "tests/data"


def main():
    protocol = "az"
    account_name = "ukmeteuwest"
    storage_options = dict(
        account_name=account_name,
        credential=planetary_computer.sas.get_token(account_name, "ukcp18").token
    )

    fs = fsspec.filesystem(protocol, **storage_options)
    paths = fs.glob(
        "ukcp18/badc/ukcp18/data/land-gcm/global/60km/rcp26/01/*/day/*/*18991201-19091130.nc"
    )
    # hmm, 'sfcWind', 'uas' and 'vas' are apparently on a different grid? size 325 instead of 324.
    # so we exclude those three variables... We *might* be able to fake it and just load them up
    # up regardless.
    paths = [x for x in paths if x.split("/")[9] not in {"sfcWind", "uas", "vas"}]
    datasets = [xr.open_dataset(fs.open(path)) for path in paths]
    DATA.mkdir(exist_ok=True)

    for path, ds in zip(paths, datasets):
        ds.chunk().to_netcdf(str(DATA / pathlib.Path(path).name), compute=False)
        print("wrote", path)


if __name__ == "__main__":
    main()