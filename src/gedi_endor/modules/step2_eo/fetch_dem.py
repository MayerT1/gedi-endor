# modules/step2_eo/fetch_dem.py

import logging
from pathlib import Path
import xarray as xr
import rioxarray
import geopandas as gpd
import s3fs

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def fetch_dem(cfg):
    """
    Fetch 10m USGS DEM from AWS, clip to AOI, and save as Zarr.

    Args:
        cfg: dict-like configuration containing:
            - geography: AOI GeoJSON/Shapefile path
            - dem:
                - product: USGS DEM product name (default 10m)
            - output_dir: directory to save raw DEM
    """
    logger.info("Starting DEM fetch from AWS...")

    aoi_path = cfg["geography"]
    dem_product = cfg.get("dem", {}).get("product", "USGS_10m_DEM")
    output_dir = Path(cfg["output_dir"]) / "dem"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"{dem_product}.zarr"

    # Load AOI
    aoi = gpd.read_file(aoi_path)
    logger.info(f"AOI loaded: {aoi_path}")

    # AWS S3 path for USGS 10m DEM tiles
    s3_bucket = "s3://usgs-dem-10m/"  # Placeholder, replace with actual AWS USGS DEM bucket
    s3 = s3fs.S3FileSystem(anon=True)

    # TODO: implement spatial filtering by AOI bounds
    # For now, this example opens one tile (replace with loop over tiles intersecting AOI)
    example_tile = f"{s3_bucket}dem_tile_example.tif"
    with s3.open(example_tile, mode="rb") as f:
        dem = rioxarray.open_rasterio(f)

    # Clip to AOI geometry
    dem_clipped = dem.rio.clip(aoi.geometry, aoi.crs, drop=True)

    # Save as Zarr
    dem_clipped.to_dataset(name="DEM").to_zarr(output_file, mode="w")
    logger.info(f"DEM saved as Zarr to {output_file}")

    logger.info("DEM fetch completed.")


if __name__ == "__main__":
    dummy_cfg = {
        "geography": "data/test_aoi.geojson",
        "dem": {"product": "USGS_10m_DEM"},
        "output_dir": "data/raw/eo"
    }
    run(dummy_cfg)
