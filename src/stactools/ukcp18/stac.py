from __future__ import annotations

import json
import logging
import collections
import datetime
from typing import Any
import urllib.parse

import pystac
import xstac
import fsspec
import xarray as xr
logger = logging.getLogger(__name__)


VARIABLES = [
    "clt",
    "hurs",
    "huss",
    "pr",
    "psl",
    "rls",
    "rss",
    "sfcWind",
    "tas",
    "tasmax",
    "tasmin",
    "uas",
    "vas",
]
TEMPORAL_RESOLUTIONS = ["day", "mon"]


Parts = collections.namedtuple(
    "Parts", "member_id variable time_resolution filename start_datetime end_datetime"
)


def parts_of(x):
    _, _, _, _, _, _, _, _, member_id, variable, time_resolution, _, filename = x.split(
        "/"
    )
    date_range = filename.split("_")[-1].rstrip(".nc").split("-")

    return Parts(
        member_id, variable, time_resolution, filename, date_range[0], date_range[1]
    )


def path_to_item_id(x):
    parts = parts_of(x)
    return "-".join(
        [
            "ukcp18",
            parts.member_id,
            parts.time_resolution,
            parts.start_datetime,
            parts.end_datetime,
        ]
    )


def get_assets_for_collection(protocol, storage_options):
    # credential = planetary_computer.sas.get_token(account_name, container_name).token
    # storage_options = dict(account_name="ukmeteuwest", credential=credential)
    # account_name = "ukmeteuwest"
    # container_name = "ukcp18"
    fs = fsspec.filesystem(protocol, **storage_options)
    paths = fs.glob(
        "ukcp18/badc/ukcp18/data/land-gcm/global/60km/rcp26/01/*/day/*/*18991201-19091130.nc"
    )
    # # hmm, 'sfcWind', 'uas' and 'vas' are apparently on a different grid? size 325 instead of 324.
    # so we exclude those three variables... We *might* be able to fake it and just load them up
    # up regardless.
    # paths = [x for x in paths if x.split("/")[9] not in {"sfcWind", "uas", "vas"}]

    datasets = [xr.open_dataset(fs.open(path)) for path in paths]
    ds = xr.merge(datasets, join="exact")
    return ds


def create_collection(ds: xr.DataArray) -> pystac.Collection:
    for k, v in ds.variables.items():
        attrs = {
            name: xr.backends.zarr.encode_zarr_attr_value(value)
            for name, value in v.attrs.items()
        }
        ds[k].attrs = attrs

    extent = pystac.Extent(
        spatial=pystac.SpatialExtent(bboxes=[[-180, -90, 180, 90]]),
        temporal=pystac.TemporalExtent(
            intervals=[datetime.datetime(1899, 12, 1), datetime.datetime(2100, 12, 31)]
        ),
    )
    keywords = ["UKCP18", "UK Met Office", "Climate"]
    extra_fields = {
        # "msft:storage_account": "ukmeteuwest",
        # "msft:container": "ukcp18",
        # "msft:short_description": (
        #     "Global climate model runs from 1900-2100 produced by the Met Office for UK Climate "
        #     "Projections 2018 (UKCP18) using the HadGEM3 climate model."
        # ),
    }
    providers = [
        pystac.Provider(
            "Met Office Hadley Centre",
            roles=[pystac.ProviderRole.PRODUCER],
            url="https://www.metoffice.gov.uk/weather/climate/met-office-hadley-centre/index",
        ),
        pystac.Provider(
            "The CEDA Archive",
            roles=[pystac.ProviderRole.HOST],
            url="https://archive.ceda.ac.uk/",
        ),
        # pystac.Provider(
        #     "Microsoft",
        #     roles=[pystac.ProviderRole.HOST, pystac.ProviderRole.PROCESSOR],
        #     url="https://planetarycomputer.microsoft.com/",
        # ),
    ]
    template = pystac.Collection(
        "ukcp-18",
        description="{{ collection.description }}",
        extent=extent,
        keywords=keywords,
        extra_fields=extra_fields,
        providers=providers,
        title="UKCP18 Global Climate Model Projections for the entire globe",
    )

    template.add_link(
        pystac.Link(
            rel=pystac.RelType.LICENSE,
            target="https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
            title="Open Government License",
            media_type="text/html",
        )
    )
    template.add_link(
        pystac.Link(
            rel="documentation",
            title="UKCP18 Guidance: Data availability, access and formats",
            target=(
                "https://www.metoffice.gov.uk/binaries/content/assets/metofficegovuk/pdf/"
                "research/ukcp/ukcp18-guidance-data-availability-access-and-formats.pdf"
            ),
            media_type="application/pdf",
        )
    )
    template.add_link(
        pystac.Link(
            rel="documentation",
            title="UKCP18 Guidance: Caveats and limitations",
            target=(
                "https://www.metoffice.gov.uk/binaries/content/assets/metofficegovuk/pdf/"
                "research/ukcp/ukcp18-guidance---caveats-and-limitations.pdf"
            ),
            media_type="application/pdf",
        )
    )
    template.add_link(
        pystac.Link(
            rel="documentation",
            title="UKCP18 Science Reports",
            target="https://www.metoffice.gov.uk/research/approach/collaboration/ukcp/guidance-science-reports",
            media_type="text/html",
        )
    )
    template.add_link(
        pystac.Link(
            rel="documentation",
            title="CEDA Archive dataset",
            target="https://catalogue.ceda.ac.uk/uuid/97bc0c622a24489aa105f5b8a8efa3f0",
            media_type="text/html",
        )
    )

    r = xstac.xarray_to_stac(ds, template, reference_system=4326)

    # We only loaded metadata for the first year. It actually runs through 2100.
    ext = pystac.extensions.datacube.DatacubeExtension.ext(r)
    time = ext.dimensions["time"]
    time.extent[1] = extent.temporal.intervals[0][1].isoformat() + "Z"

    definitions = {}
    for k, v in ext.variables.items():
        asset = pystac.extensions.item_assets.AssetDefinition({})
        asset.description = v.description
        asset.title = v.properties["attrs"].get("long_name", k)
        asset.media_type = "application/netcdf"
        asset.roles = ["data"]
        definitions[k] = asset

    # Item assets
    item_assets = pystac.extensions.item_assets.ItemAssetsExtension.ext(
        r, add_if_missing=True
    )

    item_assets.item_assets = definitions

    # Assets aren't cloned, so set it here
    # r.add_asset(
    #     "thumbnail",
    #     pystac.Asset(
    #         "https://ai4edatasetspublicassets.blob.core.windows.net/assets/pc_thumbnails/nasa-nex-gddp-thumbnail.png",
    #         title="thumbnail",
    #         media_type=pystac.MediaType.PNG,
    #         roles=["thumbnail"],
    #     ),
    # )

    sci_ext = pystac.extensions.scientific.ScientificExtension.ext(
        r, add_if_missing=True
    )
    sci_ext.citation = (
        "Met Office Hadley Centre (2018): UKCP18 Global Climate Model Projections for the entire "
        "globe. Centre for Environmental Data Analysis, date of citation. "
        "http://catalogue.ceda.ac.uk/uuid/f1a2fc3c120f400396a92f5de84d596a"
    )

    # Summaries
    r.summaries.maxcount = 50
    summaries = {
        "ukcp18:variable": VARIABLES,
        "ukcp18:temporal_resolution": TEMPORAL_RESOLUTIONS,
    }
    for k, v in summaries.items():
        r.summaries.add(k, v)

    r.validate()
    return r


def create_item(files, protocol, asset_href_transform=None, **storage_options: dict[str, Any]) -> pystac.Item:
    fs = fsspec.filesystem(protocol, **storage_options)
    ds = xr.open_mfdataset([fs.open(f) for f in files], join="exact", engine="h5netcdf")

    path = files[0]

    for k, v in ds.variables.items():
        attrs = {
            name: xr.backends.zarr.encode_zarr_attr_value(value)
            for name, value in v.attrs.items()
        }
        ds[k].attrs = attrs

    parts = parts_of(path)
    fmt = "%Y%m%d"
    start_datetime = (
        datetime.datetime.strptime(parts.start_datetime, fmt).isoformat() + "Z"
    )
    end_datetime = datetime.datetime.strptime(parts.end_datetime, fmt).isoformat() + "Z"

    template = pystac.Item(
        "item",
        geometry={
            "type": "Polygon",
            "coordinates": [
                [
                    [180.0, -90.0],
                    [180.0, 90.0],
                    [-180.0, 90.0],
                    [-180.0, -90.0],
                    [180.0, -90.0],
                ]
            ],
        },
        bbox=[-180, -90, 180, 90],
        datetime=None,
        properties={"start_datetime": start_datetime, "end_datetime": end_datetime},
    )

    item = xstac.xarray_to_stac(ds, template, reference_system=4326)
    item.id = path_to_item_id(path)

    if asset_href_transform is None:
        asset_href_transform = lambda x: x

    for path in files:
        asset = pystac.Asset(
            asset_href_transform(path),
            media_type="application/netcdf",
            roles=["data"],
        )
        item.add_asset("data", asset)

    item.properties["ukcp18:member_id"] = int(parts.member_id)
    item.properties["ukcp18:time_resolution"] = parts.time_resolution

    item.validate()
    return item