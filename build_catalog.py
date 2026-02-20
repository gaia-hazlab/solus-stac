"""Build a static STAC catalog for SOLUS100 soil property maps.

Uses rio-stac to extract projection metadata from public COGs hosted at
https://storage.googleapis.com/solus100pub/ and pystac to assemble the
catalog/collection/item hierarchy.

Catalog structure (see README.md):
    Catalog
    ├── Collection: soil_thickness
    │   ├── Item: anylithicdpt  (4 assets)
    │   └── Item: resdept       (4 assets)
    ├── Collection: depth_0cm
    │   ├── Item: caco3         (4 assets)
    │   ├── Item: claytotal     (4 assets)
    │   └── ...
    ├── Collection: depth_5cm
    │   └── ...
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

# Depths that belong to individual depth collections
DEPTH_VALUES = ["0_cm", "5_cm", "15_cm", "30_cm", "60_cm", "100_cm", "150_cm"]

# Depths that belong to the soil_thickness collection (not depth-specific)
THICKNESS_DEPTHS = ["NA", "all_cm"]

# Map filetype column values to short asset key names
FILETYPE_TO_KEY = {
    "prediction": "p",
    "95% high prediction interval": "h",
    "95% low prediction interval": "l",
    "relative prediction interval": "rpi",
}

FILETYPE_TO_ROLES = {
    "prediction": ["data"],
    "95% high prediction interval": ["data"],
    "95% low prediction interval": ["data"],
    "relative prediction interval": ["data"],
}

FILETYPE_TO_TITLE = {
    "prediction": "Prediction",
    "95% high prediction interval": "95% High Prediction Interval",
    "95% low prediction interval": "95% Low Prediction Interval",
    "relative prediction interval": "Relative Prediction Interval",
}

PROJ_EXT_URL = (
    f"https://stac-extensions.github.io/projection/{PROJECTION_EXT_VERSION}/schema.json"
)


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
    return {
        "bbox": geom_info["bbox"],
        "geometry": geom_info["footprint"],
        "proj_properties": {f"proj:{k}": v for k, v in proj_info.items()},
    }


# ---------------------------------------------------------------------------
# 3. Build a multi-asset STAC Item for one variable at one depth
# ---------------------------------------------------------------------------


def create_item(
    variable: str,
    rows: pd.DataFrame,
    proj_props: dict,
) -> pystac.Item:
    """Create a pystac.Item for a single soil variable with 4 COG assets.

    Parameters
    ----------
    variable : str
        Soil property name (e.g. ``"caco3"``).
    rows : pd.DataFrame
        Subset of the layer table for this variable+depth (4 rows).
    proj_props : dict
        Shared projection properties returned by :func:`get_proj_properties`.

    Returns
    -------
    pystac.Item
    """
    first = rows.iloc[0]
    description = first["description"]
    units = first["units"]

    release_dt = datetime.datetime(2023, 12, 15, tzinfo=datetime.timezone.utc)

    properties = {
        **proj_props["proj_properties"],
        "description": f"{description} ({units})",
        "start_datetime": release_dt.isoformat(),
        "end_datetime": release_dt.isoformat(),
    }

    assets: dict[str, pystac.Asset] = {}
    for _, row in rows.iterrows():
        key = FILETYPE_TO_KEY[row["filetype"]]
        assets[key] = pystac.Asset(
            href=row["href"],
            title=FILETYPE_TO_TITLE[row["filetype"]],
            media_type=pystac.MediaType.COG,
            roles=FILETYPE_TO_ROLES[row["filetype"]],
            extra_fields={"scalar": int(row["scalar"])},
        )

    item = pystac.Item(
        id=variable,
        geometry=proj_props["geometry"],
        bbox=proj_props["bbox"],
        datetime=None,
        properties=properties,
        stac_extensions=[PROJ_EXT_URL],
        assets=assets,
    )

    return item


# ---------------------------------------------------------------------------
# 4. Build a Collection
# ---------------------------------------------------------------------------


def create_depth_collection(
    depth: str,
    items: list[pystac.Item],
) -> pystac.Collection:
    """Create a pystac.Collection for a specific depth.

    Parameters
    ----------
    depth : str
        Depth label, e.g. ``"0_cm"`` or ``"soil_thickness"``.
    items : list[pystac.Item]
        Items belonging to this collection.

    Returns
    -------
    pystac.Collection
    """
    if depth == "soil_thickness":
        collection_id = "soil_thickness"
        title = "SOLUS100 – Soil Thickness Properties"
        description = (
            "Depth to bedrock and depth to restriction layers from the "
            "SOLUS100 100-meter soil property maps."
        )
    else:
        depth_num = depth.replace("_cm", "")
        collection_id = f"depth_{depth_num}cm"
        title = f"SOLUS100 – Depth {depth_num} cm"
        description = (
            f"Soil property predictions at {depth_num} cm depth from the "
            "SOLUS100 100-meter soil property maps."
        )

    # Derive spatial extent from the first item (all items share the same grid)
    bbox = items[0].bbox
    spatial_extent = pystac.SpatialExtent(bboxes=[bbox])

    temporal_extent = pystac.TemporalExtent(
        intervals=[[
            datetime.datetime(2023, 12, 15, tzinfo=datetime.timezone.utc),
            datetime.datetime(2023, 12, 15, tzinfo=datetime.timezone.utc),
        ]]
    )

    collection = pystac.Collection(
        id=collection_id,
        title=title,
        description=description,
        extent=pystac.Extent(spatial=spatial_extent, temporal=temporal_extent),
        license="CC-BY-4.0",
        stac_extensions=[PROJ_EXT_URL],
    )

    for item in items:
        collection.add_item(item)

    return collection


# ---------------------------------------------------------------------------
# 5. Build the full Catalog
# ---------------------------------------------------------------------------


def build_catalog(df: pd.DataFrame, proj_props: dict) -> pystac.Catalog:
    """Assemble the full SOLUS STAC catalog.

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

    # --- soil_thickness collection (NA and all_cm depths) ---
    thickness_df = df[df["depth"].isin(THICKNESS_DEPTHS)]
    if not thickness_df.empty:
        thickness_items = []
        for variable, var_rows in thickness_df.groupby("property"):
            item = create_item(variable, var_rows, proj_props)
            thickness_items.append(item)
            logger.info("  Item: %s (%d assets)", variable, len(item.assets))

        collection = create_depth_collection("soil_thickness", thickness_items)
        catalog.add_child(collection)
        logger.info("Collection: %s (%d items)", collection.id, len(thickness_items))

    # --- depth collections ---
    for depth in DEPTH_VALUES:
        depth_df = df[df["depth"] == depth]
        if depth_df.empty:
            continue

        depth_items = []
        for variable, var_rows in depth_df.groupby("property"):
            item = create_item(variable, var_rows, proj_props)
            depth_items.append(item)
            logger.info("  Item: %s (%d assets)", variable, len(item.assets))

        collection = create_depth_collection(depth, depth_items)
        catalog.add_child(collection)
        logger.info("Collection: %s (%d items)", collection.id, len(depth_items))

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

    # Summary
    n_collections = len(list(catalog.get_children()))
    n_items = len(list(catalog.get_items(recursive=True)))
    logger.info("Done – %d collections, %d items", n_collections, n_items)


if __name__ == "__main__":
    main()
