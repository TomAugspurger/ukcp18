from __future__ import annotations

import dataclasses
import datetime
import logging
import pathlib
import re

import fsspec
import pystac
import xarray as xr
import xstac

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


_xpr = re.compile(
    r"(?P<variable>\w+)_"
    r"(?P<scenario>\w+)_land-gcm_global_60km_"
    r"(?P<member_id>\d+)_"
    r"(?P<temporal_resolution>[^_]+)_"
    r"(?P<start_datetime>\d{8})-"
    r"(?P<end_datetime>\d{8}).nc"
)


@dataclasses.dataclass()
class Parts:
    scenario: str
    member_id: int
    variable: str
    temporal_resolution: str
    start_datetime: datetime.datetime
    end_datetime: datetime.datetime
    filename: str

    @classmethod
    def from_filename(cls, filename: str) -> "Parts":
        filename = pathlib.Path(filename).name
        m = _xpr.match(filename)
        if not m:
            raise ValueError(filename)
        d = m.groupdict()
        d["member_id"] = int(d["member_id"])
        fmt = "%Y%m%d"
        d["start_datetime"] = datetime.datetime.strptime(d["start_datetime"], fmt)
        d["end_datetime"] = datetime.datetime.strptime(d["end_datetime"], fmt)
        d["filename"] = filename

        return cls(**d)

    @property
    def item_id(self):
        return "-".join(
            [
                "ukcp18",
                self.temporal_resolution,
                self.scenario,
                str(self.member_id),
                self.start_datetime.isoformat() + "Z",
                self.end_datetime.isoformat() + "Z",
            ]
        )


def align(datasets, base):
    datasets = [x.copy().drop_vars("height", errors="ignore") for x in datasets]
    to_merge = []
    for x in datasets:
        if set(x.data_vars) & {"sfcWind", "uas", "vas"}:
            x = x.isel(latitude=slice(1, None))
            x["latitude"] = base["latitude"]
            x["longitude"] = base["longitude"]
            x["latitude_bnds"] = base["latitude_bnds"]
            x["longitude_bnds"] = base["longitude_bnds"]
            to_merge.append(x)
        else:
            to_merge.append(x)
    return to_merge


def merge(datasets, base):
    """
    Merge multiple datasets, accounting for differences in

    - latitude
    - longitude
    - height

    It's not clear whether the datacube extension will / should be OK with
    this. We're lying about the extents of the latitude and longitude.
    """
    to_merge = align(datasets, base)
    return xr.merge(to_merge, join="exact")


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
            target=(
                "https://www.metoffice.gov.uk/research/approach/collaboration/ukcp/"
                "guidance-science-reports"
            ),
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


def create_item(urls, storage_options=None, asset_extra_fields=None) -> pystac.Item:
    """
    Create items for a set of related assets.
    """
    storage_options = storage_options or {}
    datasets = [
        xr.open_dataset(
            fsspec.open(f, **storage_options).open(), engine="h5netcdf", chunks={}
        )
        for f in urls
    ]
    ds = merge(datasets, base=datasets[0])
    path = urls[0]

    for k, v in ds.variables.items():
        attrs = {
            name: xr.backends.zarr.encode_zarr_attr_value(value)
            for name, value in v.attrs.items()
        }
        ds[k].attrs = attrs

    parts = Parts.from_filename(path)
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
        properties={
            "start_datetime": parts.start_datetime.isoformat() + "Z",
            "end_datetime": parts.end_datetime.isoformat() + "Z",
        },
    )

    item = xstac.xarray_to_stac(ds, template, reference_system=4326)
    item.id = parts.item_id

    for path in urls:
        parts = Parts.from_filename(path)
        asset = pystac.Asset(
            path,
            media_type="application/netcdf",
            roles=["data"],
            extra_fields=asset_extra_fields,
        )
        item.add_asset(parts.variable, asset)

    item.properties["ukcp18:member_id"] = parts.member_id
    item.properties["ukcp18:temporal_resolution"] = parts.temporal_resolution

    item.validate()
    return item
