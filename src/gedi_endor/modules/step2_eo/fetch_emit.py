# modules/step2_eo/fetch_emit.py

import logging
from pathlib import Path
import intake
import xarray as xr
import rioxarray
import geopandas as gpd

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def fetch_emit(cfg):
    """
    Fetch EMIT L2A Reflectance (EMITL2ARFL) data for given AOI and timeframe,
    store as Zarr for later CWC/EWT computation.

    Args:
        cfg: dict-like configuration containing:
            - geography: AOI GeoJSON/Shapefile
            - eo:
                - timeframe: dict with 'start' and 'end'
            - output_dir: base directory to store raw EMIT data
    """
    logger.info("Starting EMIT fetch...")

    aoi_path = cfg["geography"]
    timeframe = cfg["eo"]["timeframe"]
    output_dir = Path(cfg["output_dir"]) / "emit"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load AOI
    aoi = gpd.read_file(aoi_path)
    if aoi.crs is None:
        aoi.set_crs("EPSG:4326", inplace=True)

    # Access EMIT L2A Reflectance catalog via short name
    short_name = "EMITL2ARFL"
    catalog_url = f"https://opendap.earthdata.nasa.gov/podaac-ops/EMIT_L2A_Reflectance/catalog.yaml"

    logger.info(f"Accessing EMIT catalog for short name {short_name}: {catalog_url}")
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
    logger.info(f"Saved raw EMIT data to Zarr: {zarr_path}")

    logger.info("EMIT fetch completed.")


if __name__ == "__main__":
    dummy_cfg = {
        "geography": "data/test_aoi.geojson",
        "eo": {
            "timeframe": {"start": "2023-01-01", "end": "2023-12-31"}
        },
        "output_dir": "data/raw/eo"
    }
    run(dummy_cfg)
