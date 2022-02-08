import datetime
import pathlib

import pytest
import stactools.ukcp18.stac
import xarray as xr


HERE = pathlib.Path(__file__).parent
DATA = HERE / "data"


@pytest.mark.parametrize("filename", [
    "tasmax_rcp26_land-gcm_global_60km_01_day_18991201-19091130.nc",
    "directory/tasmax_rcp26_land-gcm_global_60km_01_day_18991201-19091130.nc",
])
def test_parts(filename):
    result = stactools.ukcp18.stac.Parts.from_filename(filename)
    assert result.scenario == "rcp26"
    assert result.member_id == 1
    assert result.temporal_resolution == "day"
    assert result.start_datetime == datetime.datetime(1899, 12, 1)
    assert result.end_datetime == datetime.datetime(1909, 11, 30)


def test_create_collection():
    datasets = [xr.open_dataset(p) for p in DATA.glob("*.nc")]
    ds = stactools.ukcp18.stac.merge(datasets, base=datasets[0])

    collection = stactools.ukcp18.stac.create_collection(ds)
    assert collection.keywords
    assert collection.summaries


def test_create_item():
    urls = [
        f"file://{p}" for p in DATA.glob("*.nc")
    ]
    item = stactools.ukcp18.stac.create_item(urls)
    assert item.assets