"""Build a static STAC catalog for SOLUS100 soil property maps.

Uses rio-stac to extract projection metadata from public COGs hosted at
https://storage.googleapis.com/solus100pub/ and pystac to assemble the
catalog/collection/item hierarchy.

Catalog structure (see README.md):
    Catalog
    ├── Collection: soil_thickness
    │   ├── Item: anylithicdpt
    │   │   ├── Asset: p
    │   │   ├── Asset: l
    │   │   ├── Asset: h
    │   │   └── Asset: rpi
    │   └── Item: resdept
    │       └── ...
    ├── Collection: p  (prediction)
    │   ├── Item: depth_0cm
    │   │   ├── Asset: caco3
    │   │   ├── Asset: sandco
    │   │   └── ...
    │   ├── Item: depth_5cm
    │   │   └── ...
    │   └── ...
    ├── Collection: l  (95% low prediction interval)
    │   └── ...
    ├── Collection: h  (95% high prediction interval)
    │   └── ...
    └── Collection: rpi  (relative prediction interval)
        └── ...
"""

from __future__ import annotations

import datetime
import logging
from pathlib import Path

import pandas as pd
import pystac
import rasterio
from rio_stac.stac import (
    PROJECTION_EXT_VERSION,
    get_dataset_geom,
    get_projection_info,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

BUCKET_URL = "https://storage.googleapis.com/solus100pub"
CSV_URL = f"{BUCKET_URL}/Final_Layer_Table_20231215.csv"
CATALOG_DIR = Path("stac")

# Depths that belong to individual depth items
DEPTH_VALUES = ["0_cm", "5_cm", "15_cm", "30_cm", "60_cm", "100_cm", "150_cm"]

# Depths that belong to the soil_thickness collection (not depth-specific)
THICKNESS_DEPTHS = ["NA", "all_cm"]

# Ordered estimate-type keys used as top-level collection IDs
ESTIMATE_KEYS = ["p", "l", "h", "rpi"]

# Full metadata for each estimate type, keyed by the short ID
ESTIMATE_TYPES: dict[str, dict] = {
    "p": {
        "filetype": "prediction",
        "collection_id": "p",
        "collection_title": "SOLUS100 – Prediction",
        "collection_description": (
            "Best-estimate (predicted) soil property values from SOLUS100 "
            "100-meter resolution maps."
        ),
        "asset_title": "Prediction",
        "roles": ["data"],
    },
    "l": {
        "filetype": "95% low prediction interval",
        "collection_id": "l",
        "collection_title": "SOLUS100 – Low Prediction Interval",
        "collection_description": (
            "95% lower prediction interval for SOLUS100 soil property values."
        ),
        "asset_title": "95% Low Prediction Interval",
        "roles": ["data"],
    },
    "h": {
        "filetype": "95% high prediction interval",
        "collection_id": "h",
        "collection_title": "SOLUS100 – High Prediction Interval",
        "collection_description": (
            "95% upper prediction interval for SOLUS100 soil property values."
        ),
        "asset_title": "95% High Prediction Interval",
        "roles": ["data"],
    },
    "rpi": {
        "filetype": "relative prediction interval",
        "collection_id": "rpi",
        "collection_title": "SOLUS100 – Relative Prediction Interval",
        "collection_description": (
            "Relative prediction interval (uncertainty) for SOLUS100 soil property values."
        ),
        "asset_title": "Relative Prediction Interval",
        "roles": ["data"],
    },
}

PROJ_EXT_URL = (
    f"https://stac-extensions.github.io/projection/{PROJECTION_EXT_VERSION}/schema.json"
)
RASTER_EXT_URL = "https://stac-extensions.github.io/raster/v1.1.0/schema.json"


# ---------------------------------------------------------------------------
# 1. Load and parse the layer table
# ---------------------------------------------------------------------------


def load_layer_table(csv_url: str = CSV_URL) -> pd.DataFrame:
    """Download and return the SOLUS layer table as a DataFrame.

    Adds an ``href`` column with the full GCS URL for each COG.
    The CSV contains ``_2D_`` in a few filenames (e.g. ``anylithicdpt``)
    that do not match the actual bucket objects, so we strip that part.
    """
    df = pd.read_csv(csv_url, keep_default_na=False)
    df["filename"] = df["filename"].str.replace("_2D_", "_", regex=False)
    df["href"] = df["filename"].apply(lambda f: f"{BUCKET_URL}/{f}")
    logger.info("Loaded %d rows from %s", len(df), csv_url)
    return df


# ---------------------------------------------------------------------------
# 2. Derive projection properties from a single representative COG
# ---------------------------------------------------------------------------


def get_proj_properties(href: str) -> dict:
    """Open a COG via GDAL's /vsicurl/ and return STAC projection properties.

    All SOLUS COGs share the same grid, so we only need to call this once.
    """
    vsicurl = f"/vsicurl/{href}"
    with rasterio.open(vsicurl) as src:
        geom_info = get_dataset_geom(src)
        proj_info = get_projection_info(src)
        dtype: str = src.dtypes[0]
        raw_nodata = src.nodata
        nodata = int(raw_nodata) if raw_nodata is not None and raw_nodata == int(raw_nodata) else raw_nodata
    return {
        "bbox": geom_info["bbox"],
        "geometry": geom_info["footprint"],
        "proj_properties": {f"proj:{k}": v for k, v in proj_info.items()},
        "dtype": dtype,
        "nodata": nodata,
    }


# ---------------------------------------------------------------------------
# 3. Build STAC Items
# ---------------------------------------------------------------------------


def _make_release_dt() -> datetime.datetime:
    return datetime.datetime(2023, 12, 15, tzinfo=datetime.timezone.utc)


def _base_properties(proj_props: dict) -> dict:
    release_dt = _make_release_dt()
    return {
        **proj_props["proj_properties"],
        "start_datetime": release_dt.isoformat(),
        "end_datetime": release_dt.isoformat(),
    }


def _raster_band(proj_props: dict, scalar: int, units: str) -> dict:
    """Build a ``raster:bands`` entry.

    When *scalar* is not 1, the stored integer value must be divided by
    *scalar* to recover the physical quantity, so ``scale = 1 / scalar``
    and ``offset = 0`` are added per the STAC Raster Extension spec.
    """
    band: dict = {
        "data_type": proj_props["dtype"],
        "nodata": proj_props["nodata"],
        "unit": units,
    }
    if scalar != 1:
        band["scale"] = 1 / scalar
        band["offset"] = 0
    return band


def create_depth_item(
    depth: str,
    depth_df: pd.DataFrame,
    estimate_key: str,
    proj_props: dict,
) -> pystac.Item:
    """Create a pystac.Item for one depth with one Asset per soil variable.

    Parameters
    ----------
    depth : str
        Depth label, e.g. ``"0_cm"``.
    depth_df : pd.DataFrame
        Rows from the layer table for this depth and estimate type (one row
        per soil variable).
    estimate_key : str
        Short estimate-type key (``"p"``, ``"l"``, ``"h"``, or ``"rpi"``).
    proj_props : dict
        Shared projection properties returned by :func:`get_proj_properties`.

    Returns
    -------
    pystac.Item
    """
    depth_num = depth.replace("_cm", "")
    item_id = f"depth_{depth_num}cm"
    depth_cm = int(depth_num)
    est = ESTIMATE_TYPES[estimate_key]

    properties = _base_properties(proj_props)
    properties["depth"] = depth_cm

    assets: dict[str, pystac.Asset] = {}
    for _, row in depth_df.iterrows():
        variable = row["property"]
        scalar = int(row["scalar"])
        assets[variable] = pystac.Asset(
            href=row["href"],
            title=f"{row['description']} ({row['units']})",
            media_type=pystac.MediaType.COG,
            roles=est["roles"],
            extra_fields={
                "raster:bands": [_raster_band(proj_props, scalar, row["units"])],
            },
        )

    return pystac.Item(
        id=item_id,
        geometry=proj_props["geometry"],
        bbox=proj_props["bbox"],
        datetime=None,
        properties=properties,
        stac_extensions=[PROJ_EXT_URL, RASTER_EXT_URL],
        assets=assets,
    )


def create_thickness_item(
    estimate_key: str,
    est_df: pd.DataFrame,
    variable_descriptions: dict[str, str],
    proj_props: dict,
) -> pystac.Item:
    """Create a pystac.Item for one estimate type with one asset per thickness variable.

    Parameters
    ----------
    estimate_key : str
        Short estimate-type key (``"p"``, ``"l"``, ``"h"``, or ``"rpi"``).
    est_df : pd.DataFrame
        All rows for this estimate type (one per thickness variable).
    variable_descriptions : dict[str, str]
        Mapping of variable name → human-readable description string.
    proj_props : dict
        Shared projection properties.

    Returns
    -------
    pystac.Item
    """
    est = ESTIMATE_TYPES[estimate_key]
    properties = _base_properties(proj_props)

    assets: dict[str, pystac.Asset] = {}
    for _, row in est_df.iterrows():
        variable = row["property"]
        scalar = int(row["scalar"])
        assets[variable] = pystac.Asset(
            href=row["href"],
            title=variable_descriptions.get(variable, variable),
            media_type=pystac.MediaType.COG,
            roles=est["roles"],
            extra_fields={
                "raster:bands": [_raster_band(proj_props, scalar, row["units"])],
            },
        )

    return pystac.Item(
        id=estimate_key,
        geometry=proj_props["geometry"],
        bbox=proj_props["bbox"],
        datetime=None,
        properties=properties,
        stac_extensions=[PROJ_EXT_URL, RASTER_EXT_URL],
        assets=assets,
    )


# ---------------------------------------------------------------------------
# 4. Build Collections
# ---------------------------------------------------------------------------


def _make_temporal_extent() -> pystac.TemporalExtent:
    release_dt = _make_release_dt()
    return pystac.TemporalExtent(intervals=[[release_dt, release_dt]])


def _spatial_extent(items: list[pystac.Item]) -> pystac.SpatialExtent:
    return pystac.SpatialExtent(bboxes=[items[0].bbox])


def create_estimate_collection(
    estimate_key: str,
    items: list[pystac.Item],
    variable_descriptions: dict[str, str],
) -> pystac.Collection:
    """Create a top-level Collection for one estimate type.

    Items are one per depth; each Item contains one Asset per soil variable.
    ``item_assets`` is populated with one entry per variable so that clients
    know the full set of available assets without inspecting individual Items.

    Parameters
    ----------
    estimate_key : str
        Short estimate-type key (``"p"``, ``"l"``, ``"h"``, or ``"rpi"``).
    items : list[pystac.Item]
        One Item per depth level, each with assets keyed by variable name.
    variable_descriptions : dict[str, str]
        Mapping of variable name → human-readable description string
        (e.g. ``"caco3" → "Calcium carbonate (percent mass)"``).

    Returns
    -------
    pystac.Collection
    """
    est = ESTIMATE_TYPES[estimate_key]
    collection = pystac.Collection(
        id=est["collection_id"],
        title=est["collection_title"],
        description=est["collection_description"],
        extent=pystac.Extent(
            spatial=_spatial_extent(items),
            temporal=_make_temporal_extent(),
        ),
        license="CC-BY-4.0",
        stac_extensions=[PROJ_EXT_URL, RASTER_EXT_URL],
    )

    # item_assets: advertise every variable asset so odc.stac (and other
    # clients) can discover the full band list from the collection alone.
    for variable, var_description in sorted(variable_descriptions.items()):
        collection.item_assets[variable] = pystac.ItemAssetDefinition.create(
            title=var_description,
            description=est["collection_description"],
            media_type=pystac.MediaType.COG,
            roles=est["roles"],
        )

    for item in items:
        collection.add_item(item)
    return collection


def create_thickness_collection(
    items: list[pystac.Item],
    variable_descriptions: dict[str, str],
) -> pystac.Collection:
    """Create the soil_thickness Collection.

    Items are one per estimate type (p/l/h/rpi); each Item has one asset per
    thickness variable (anylithicdpt, resdept).

    Parameters
    ----------
    items : list[pystac.Item]
        One Item per estimate type.
    variable_descriptions : dict[str, str]
        Mapping of variable name → human-readable description string.

    Returns
    -------
    pystac.Collection
    """
    collection = pystac.Collection(
        id="soil_thickness",
        title="SOLUS100 – Soil Thickness",
        description=(
            "Depth to bedrock and depth to restriction layers from the "
            "SOLUS100 100-meter soil property maps."
        ),
        extent=pystac.Extent(
            spatial=_spatial_extent(items),
            temporal=_make_temporal_extent(),
        ),
        license="CC-BY-4.0",
        stac_extensions=[PROJ_EXT_URL, RASTER_EXT_URL],
    )

    # item_assets: one entry per thickness variable (shared across all estimate items)
    for variable, var_description in sorted(variable_descriptions.items()):
        collection.item_assets[variable] = pystac.ItemAssetDefinition.create(
            title=var_description,
            description=var_description,
            media_type=pystac.MediaType.COG,
            roles=["data"],
        )

    for item in items:
        collection.add_item(item)
    return collection


# ---------------------------------------------------------------------------
# 5. Build the full Catalog
# ---------------------------------------------------------------------------


def build_catalog(df: pd.DataFrame, proj_props: dict) -> pystac.Catalog:
    """Assemble the full SOLUS STAC catalog.

    Structure:
    - One ``soil_thickness`` Collection: one Item per estimate type (p/l/h/rpi),
      each with 2 assets (anylithicdpt, resdept).
    - One Collection per estimate type (p/l/h/rpi): one Item per depth,
      one Asset per soil variable.

    Parameters
    ----------
    df : pd.DataFrame
        Full layer table.
    proj_props : dict
        Shared projection properties.

    Returns
    -------
    pystac.Catalog
    """
    catalog = pystac.Catalog(
        id="solus100",
        title="Soil Landscapes of the United States 100-meter (SOLUS100)",
        description=(
            "STAC catalog for SOLUS100 soil property maps. "
            "100-meter resolution predictions of soil properties across the "
            "contiguous United States. "
            "See https://storage.googleapis.com/solus100pub/index.html"
        ),
    )

    # --- soil_thickness collection ---
    # Items: one per estimate type (p/l/h/rpi); each Item has one asset per thickness variable.
    thickness_df = df[df["depth"].isin(THICKNESS_DEPTHS)]
    if not thickness_df.empty:
        # Build variable→description lookup from thickness rows
        thickness_var_descriptions: dict[str, str] = {
            row["property"]: f"{row['description']} ({row['units']})"
            for _, row in thickness_df.drop_duplicates("property").iterrows()
        }
        thickness_items: list[pystac.Item] = []
        for estimate_key in ESTIMATE_KEYS:
            est = ESTIMATE_TYPES[estimate_key]
            est_df = thickness_df[thickness_df["filetype"] == est["filetype"]]
            if est_df.empty:
                continue
            item = create_thickness_item(estimate_key, est_df, thickness_var_descriptions, proj_props)
            thickness_items.append(item)
            logger.info("  [soil_thickness] Item: %s (%d assets)", estimate_key, len(item.assets))
        thickness_col = create_thickness_collection(thickness_items, thickness_var_descriptions)
        catalog.add_child(thickness_col)
        logger.info("Collection: %s (%d items)", thickness_col.id, len(thickness_items))

    # --- per-estimate-type collections ---
    # Items: one per depth; each Item has one asset per soil variable.
    #
    # Build a variable→description lookup once from any depth/filetype combo.
    variable_descriptions: dict[str, str] = {
        row["property"]: f"{row['description']} ({row['units']})"
        for _, row in df[df["depth"] == DEPTH_VALUES[0]].iterrows()
    }

    for estimate_key in ESTIMATE_KEYS:
        est = ESTIMATE_TYPES[estimate_key]
        filetype = est["filetype"]
        est_df = df[df["filetype"] == filetype]

        depth_items: list[pystac.Item] = []
        for depth in DEPTH_VALUES:
            depth_df = est_df[est_df["depth"] == depth]
            if depth_df.empty:
                continue
            item = create_depth_item(depth, depth_df, estimate_key, proj_props)
            depth_items.append(item)
            logger.info(
                "  [%s] Item: %s (%d assets)", estimate_key, item.id, len(item.assets)
            )

        est_col = create_estimate_collection(estimate_key, depth_items, variable_descriptions)
        catalog.add_child(est_col)
        logger.info("Collection: %s (%d items)", est_col.id, len(depth_items))

    return catalog


# ---------------------------------------------------------------------------
# 6. Save the catalog
# ---------------------------------------------------------------------------


def save_catalog(catalog: pystac.Catalog, dest_dir: Path = CATALOG_DIR) -> None:
    """Normalize and save the catalog as self-contained JSON files.

    Parameters
    ----------
    catalog : pystac.Catalog
        The assembled STAC catalog.
    dest_dir : Path
        Output directory (will be created if needed).
    """
    catalog.normalize_hrefs(str(dest_dir))
    catalog.validate_all()
    catalog.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)
    logger.info("Catalog saved to %s/", dest_dir)


# ---------------------------------------------------------------------------
# 7. Save all items as a pystac ItemCollection (GeoJSON FeatureCollection)
# ---------------------------------------------------------------------------


def save_item_collection(
    catalog: pystac.Catalog,
    dest_dir: Path = CATALOG_DIR,
) -> None:
    """Collect every Item in the catalog and write a GeoJSON FeatureCollection.

    Parameters
    ----------
    catalog : pystac.Catalog
        The assembled (and already normalized) STAC catalog.
    dest_dir : Path
        Output directory; the file is written as ``item_collection.json``.
    """
    items = list(catalog.get_items(recursive=True))
    item_collection = pystac.ItemCollection(items=items)
    out_path = dest_dir / "item_collection.json"
    item_collection.save_object(dest_href=str(out_path))
    logger.info(
        "ItemCollection saved to %s (%d items)", out_path, len(items)
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Entry point: load CSV, fetch projection info, build & save catalog."""
    df = load_layer_table()

    # All SOLUS COGs share the same grid – pick any one to read proj metadata
    sample_href = df["href"].iloc[0]
    logger.info("Reading projection info from %s …", sample_href)
    proj_props = get_proj_properties(sample_href)
    logger.info("Bounding box: %s", proj_props["bbox"])

    catalog = build_catalog(df, proj_props)
    save_catalog(catalog)
    save_item_collection(catalog)

    # Summary
    n_collections = len(list(catalog.get_children()))
    n_items = len(list(catalog.get_items(recursive=True)))
    logger.info("Done – %d collections, %d items", n_collections, n_items)


if __name__ == "__main__":
    main()
