# stactools.ukcp18

stactools package for UKCP18

## Usage

Load the metadata

```python
>>> import planetary_computer
>>> import adlfs
>>> import xarray as xr

>>> account_name = "ukmeteuwest"
>>> container_name = "ukcp18"
>>> credential = planetary_computer.sas.get_token(account_name, container_name).token

>>> fs = adlfs.AzureBlobFileSystem("ukmeteuwest", credential=credential)
>>> paths = fs.glob(
...     "ukcp18/badc/ukcp18/data/land-gcm/global/60km/rcp26/01/*/day/*/*18991201-19091130.nc"
... )
>>> paths = [x for x in paths if x.split("/")[9] not in {"sfcWind", "uas", "vas"}]
>>> datasets = [xr.open_dataset(fs.open(path), chunks={}) for path in paths]
>>> ds = xr.merge(datasets, join="exact")
```

Create the Collection

```python
>>> import stactools.ukcp18.stac
>>> collection = stactools.ukcp18.stac.create_collection(ds)
```

Create an Item

```python
>>> import stactools.ukcp18.stac
>>> collection = stactools.ukcp18.stac.create_collection(ds)
```
