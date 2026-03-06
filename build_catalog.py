"""Build a static STAC catalog for SOLUS100 soil property maps.

Uses rio-stac to extract projection metadata from public COGs hosted at
https://storage.googleapis.com/solus100pub/ and pystac to assemble the
catalog/collection/item hierarchy.

Catalog structure (see README.md):
    Catalog
    ├── Collection: p  (prediction)
    │   ├── Collection: depth_0cm
    │   │   ├── Item: caco3   └── Asset: caco3
    │   │   └── Item: sandco  └── Asset: sandco
    │   ├── Collection: depth_5cm
    │   │   └── ...
    │   └── Collection: soil_thickness
    │       ├── Item: anylithicdpt  └── Asset: anylithicdpt
    │       └── Item: resdept       └── Asset: resdept
    ├── Collection: h  (95% high prediction interval)
    │   └── ...
    ├── Collection: l  (95% low prediction interval)
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

# Depths that belong to individual depth collections
DEPTH_VALUES = ["0_cm", "5_cm", "15_cm", "30_cm", "60_cm", "100_cm", "150_cm"]

# Depths that belong to the soil_thickness collection (not depth-specific)
THICKNESS_DEPTHS = ["NA", "all_cm"]

# Ordered estimate-type keys used as top-level collection IDs
ESTIMATE_KEYS = ["p", "h", "l", "rpi"]

# Full metadata for each estimate type, keyed by the short ID
ESTIMATE_TYPES: dict[str, dict] = {
    "p": {
        "filetype": "prediction",
        "title": "Prediction",
        "description": "Best-estimate (predicted) soil property values from SOLUS100.",
        "roles": ["data"],
    },
    "h": {
        "filetype": "95% high prediction interval",
        "title": "95% High Prediction Interval",
        "description": "95% upper prediction interval for SOLUS100 soil property values.",
        "roles": ["data"],
    },
    "l": {
        "filetype": "95% low prediction interval",
        "title": "95% Low Prediction Interval",
        "description": "95% lower prediction interval for SOLUS100 soil property values.",
        "roles": ["data"],
    },
    "rpi": {
        "filetype": "relative prediction interval",
        "title": "Relative Prediction Interval",
        "description": "Relative prediction interval (uncertainty) for SOLUS100 soil property values.",
        "roles": ["data"],
    },
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
# 3. Build a single-asset STAC Item for one variable / depth / estimate type
# ---------------------------------------------------------------------------


def create_item(
    variable: str,
    row: pd.Series,
    estimate_key: str,
    proj_props: dict,
    depth_cm: int | None = None,
) -> pystac.Item:
    """Create a pystac.Item for one soil variable with a single COG asset.

    Parameters
    ----------
    variable : str
        Soil property name (e.g. ``"caco3"``).
    row : pd.Series
        Single row from the layer table for this variable/depth/filetype.
    estimate_key : str
        Short estimate-type key (``"p"``, ``"h"``, ``"l"``, or ``"rpi"``).
    proj_props : dict
        Shared projection properties returned by :func:`get_proj_properties`.
    depth_cm : int or None
        Depth in centimetres.  ``None`` for soil-thickness items.

    Returns
    -------
    pystac.Item
    """
    description = row["description"]
    units = row["units"]
    est = ESTIMATE_TYPES[estimate_key]

    release_dt = datetime.datetime(2023, 12, 15, tzinfo=datetime.timezone.utc)

    properties: dict = {
        **proj_props["proj_properties"],
        "description": f"{description} ({units})",
        "start_datetime": release_dt.isoformat(),
        "end_datetime": release_dt.isoformat(),
    }

    if depth_cm is not None:
        properties["depth"] = depth_cm

    asset = pystac.Asset(
        href=row["href"],
        title=est["title"],
        media_type=pystac.MediaType.COG,
        roles=est["roles"],
        extra_fields={"scalar": int(row["scalar"])},
    )

    item = pystac.Item(
        id=variable,
        geometry=proj_props["geometry"],
        bbox=proj_props["bbox"],
        datetime=None,
        properties=properties,
        stac_extensions=[PROJ_EXT_URL],
        assets={variable: asset},
    )

    return item


# ---------------------------------------------------------------------------
# 4. Build Collections
# ---------------------------------------------------------------------------


def _make_temporal_extent() -> pystac.TemporalExtent:
    release_dt = datetime.datetime(2023, 12, 15, tzinfo=datetime.timezone.utc)
    return pystac.TemporalExtent(intervals=[[release_dt, release_dt]])


def create_depth_collection(
    depth: str,
    items: list[pystac.Item],
    estimate_key: str,
) -> pystac.Collection:
    """Create a pystac.Collection for a specific depth.

    Parameters
    ----------
    depth : str
        Depth label, e.g. ``"0_cm"`` or ``"soil_thickness"``.
    items : list[pystac.Item]
        Items belonging to this collection (one per soil variable).
    estimate_key : str
        Short estimate-type key (``"p"``, ``"h"``, ``"l"``, or ``"rpi"``).
        Used to populate ``item_assets`` metadata on the collection.

    Returns
    -------
    pystac.Collection
    """
    if depth == "soil_thickness":
        collection_id = "soil_thickness"
        title = "Soil Thickness"
        description = (
            "Depth to bedrock and depth to restriction layers from the "
            "SOLUS100 100-meter soil property maps."
        )
    else:
        depth_num = depth.replace("_cm", "")
        collection_id = f"depth_{depth_num}cm"
        title = f"Depth {depth_num} cm"
        description = (
            f"Soil property predictions at {depth_num} cm depth from the "
            "SOLUS100 100-meter soil property maps."
        )

    est = ESTIMATE_TYPES[estimate_key]

    bbox = items[0].bbox
    collection = pystac.Collection(
        id=collection_id,
        title=title,
        description=description,
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent(bboxes=[bbox]),
            temporal=_make_temporal_extent(),
        ),
        license="CC-BY-4.0",
        stac_extensions=[PROJ_EXT_URL],
    )

    # Populate item_assets: one entry per variable (= asset key in member Items).
    # The description is taken from the item's own properties so it is specific
    # to each soil variable (e.g. "Silt content (percent mass)").
    for item in items:
        for asset_key in item.assets:
            collection.item_assets[asset_key] = pystac.ItemAssetDefinition.create(
                title=est["title"],
                description=item.properties.get("description"),
                media_type=pystac.MediaType.COG,
                roles=est["roles"],
            )

    for item in items:
        collection.add_item(item)
    return collection


def create_estimate_collection(
    estimate_key: str,
    depth_collections: list[pystac.Collection],
) -> pystac.Collection:
    """Create a top-level pystac.Collection for one estimate type.

    Parameters
    ----------
    estimate_key : str
        Short estimate-type key (``"p"``, ``"h"``, ``"l"``, or ``"rpi"``).
    depth_collections : list[pystac.Collection]
        Depth sub-collections belonging to this estimate type.

    Returns
    -------
    pystac.Collection
    """
    est = ESTIMATE_TYPES[estimate_key]
    bbox = next(
        item.bbox
        for col in depth_collections
        for item in col.get_items()
    )
    collection = pystac.Collection(
        id=estimate_key,
        title=f"SOLUS100 – {est['title']}",
        description=est["description"],
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent(bboxes=[bbox]),
            temporal=_make_temporal_extent(),
        ),
        license="CC-BY-4.0",
        stac_extensions=[PROJ_EXT_URL],
    )
    for depth_col in depth_collections:
        collection.add_child(depth_col)
    return collection


# ---------------------------------------------------------------------------
# 5. Build the full Catalog
# ---------------------------------------------------------------------------


def build_catalog(df: pd.DataFrame, proj_props: dict) -> pystac.Catalog:
    """Assemble the full SOLUS STAC catalog.

    The catalog is organised first by estimate type (p / h / l / rpi) and
    then by depth, so that each Item contains exactly one COG asset.

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

    for estimate_key in ESTIMATE_KEYS:
        est = ESTIMATE_TYPES[estimate_key]
        filetype = est["filetype"]
        est_df = df[df["filetype"] == filetype]

        depth_collections: list[pystac.Collection] = []

        # --- soil_thickness depth (NA / all_cm rows) ---
        thickness_df = est_df[est_df["depth"].isin(THICKNESS_DEPTHS)]
        if not thickness_df.empty:
            thickness_items: list[pystac.Item] = []
            for variable, var_rows in thickness_df.groupby("property"):
                row = var_rows.iloc[0]
                item = create_item(variable, row, estimate_key, proj_props, depth_cm=None)
                thickness_items.append(item)
                logger.info("  [%s/soil_thickness] Item: %s", estimate_key, variable)
            depth_collections.append(
                create_depth_collection("soil_thickness", thickness_items, estimate_key)
            )

        # --- numeric depth collections ---
        for depth in DEPTH_VALUES:
            depth_df = est_df[est_df["depth"] == depth]
            if depth_df.empty:
                continue
            depth_cm = int(depth.replace("_cm", ""))
            depth_items: list[pystac.Item] = []
            for variable, var_rows in depth_df.groupby("property"):
                row = var_rows.iloc[0]
                item = create_item(variable, row, estimate_key, proj_props, depth_cm=depth_cm)
                depth_items.append(item)
                logger.info("  [%s/%s] Item: %s", estimate_key, depth, variable)
            depth_collections.append(create_depth_collection(depth, depth_items, estimate_key))

        est_collection = create_estimate_collection(estimate_key, depth_collections)
        catalog.add_child(est_collection)
        n_items = sum(len(list(c.get_items())) for c in depth_collections)
        logger.info(
            "Collection: %s (%d depth groups, %d items)",
            est_collection.id,
            len(depth_collections),
            n_items,
        )

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
