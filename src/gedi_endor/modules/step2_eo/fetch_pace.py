# modules/step2_eo/fetch_pace.py

import logging
from pathlib import Path
import intake
import xarray as xr
import rioxarray
import geopandas as gpd

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def fetch_pace(cfg):
    """
    Fetch PACE L2 hyperspectral EO data using short name PACE_OCI_L2_LANDVI,
    for a given AOI and timeframe, store as Zarr.

    Args:
        cfg: dict-like configuration containing:
            - geography: AOI GeoJSON/Shapefile
            - eo:
                - products: list of desired PACE indices (Car, CCI, PRI, CIRE, mARI)
                - timeframe: dict with 'start' and 'end'
            - output_dir: base directory to store raw PACE data
    """
    logger.info("Starting PACE fetch...")

    aoi_path = cfg["geography"]
    products = cfg["eo"]["products"]  # indices will be computed later
    timeframe = cfg["eo"]["timeframe"]
    output_dir = Path(cfg["output_dir"]) / "pace"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load AOI
    aoi = gpd.read_file(aoi_path)
    if aoi.crs is None:
        aoi.set_crs("EPSG:4326", inplace=True)

    # Access NASA PACE OCI L2 catalog via short name
    short_name = "PACE_OCI_L2_LANDVI"
    catalog_url = f"https://opendap.earthdata.nasa.gov/podaac-ops/PACE_L2_LANDVI/catalog.yaml"

    logger.info(f"Accessing PACE catalog for short name {short_name}: {catalog_url}")
    try:
        cat = intake.open_catalog(catalog_url)
        ds = cat[short_name].to_dask()  # dask-backed xarray Dataset
    except KeyError:
        logger.error(f"Short name {short_name} not found in catalog.")
        return
    except Exception as e:
        logger.error(f"Failed to open catalog: {e}")
        return

    # Filter by timeframe
    ds = ds.sel(time=slice(timeframe["start"], timeframe["end"]))

    # Clip to AOI
    ds = ds.rio.write_crs("EPSG:4326", inplace=True)
    ds_clipped = ds.rio.clip(aoi.geometry.apply(lambda x: x.__geo_interface__), aoi.crs)

    # Save raw data as Zarr
    zarr_path = output_dir / f"{short_name}_{timeframe['start']}_{timeframe['end']}.zarr"
    ds_clipped.to_zarr(zarr_path, mode="w")
    logger.info(f"Saved raw PACE data to Zarr: {zarr_path}")

    logger.info("PACE fetch completed.")


if __name__ == "__main__":
    dummy_cfg = {
        "geography": "data/test_aoi.geojson",
        "eo": {
            "products": ["Car", "CCI", "PRI", "CIRE", "mARI"],  # indices to compute later
            "timeframe": {"start": "2023-01-01", "end": "2023-12-31"}
        },
        "output_dir": "data/raw/eo"
    }
    run(dummy_cfg)
