# modules/step2_eo/fetch_gedi.py

import os
import logging
from pathlib import Path
import pandas as pd
import geopandas as gpd
from datetime import datetime
from typing import List
import requests
import h5py

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def download_gedi_granule(granule_url: str, out_path: Path):
    """Download a GEDI granule from LP DAAC to the specified path."""
    logger.info(f"Downloading GEDI granule: {granule_url}")
    response = requests.get(granule_url, stream=True)
    response.raise_for_status()
    with open(out_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    logger.info(f"Saved granule to {out_path}")


def read_gedi_hdf5(file_path: Path, items_to_extract: List[str]) -> pd.DataFrame:
    """Extract selected datasets from a GEDI HDF5 file into a Pandas DataFrame."""
    data = {}
    with h5py.File(file_path, "r") as f:
        for item in items_to_extract:
            try:
                key = item.lstrip("/")
                data[key] = f[key][:].flatten()
            except KeyError:
                logger.warning(f"{item} not found in {file_path.name}, skipping.")
    df = pd.DataFrame(data)
    return df


def fetch_gedi(cfg):
    """
    Fetch GEDI L1/L2 products, filter by AOI and timeframe, and save as Parquet.

    Args:
        cfg: dict-like configuration containing:
            - geography: path to AOI GeoJSON/Shapefile
            - eo -> gedi: GEDI-specific config (products, items_to_extract, timeframe)
            - output_dir: base directory to save raw GEDI data
    """
    logger.info("Starting GEDI fetch...")

    aoi_path = cfg["geography"]
    gedi_cfg = cfg["eo"]["gedi"]  # <--- updated to use eo -> gedi
    products = gedi_cfg["products"]
    items_dict = gedi_cfg["items_to_extract"]
    timeframe = gedi_cfg["timeframe"]
    base_output_dir = Path(cfg["output_dir"])
    base_output_dir.mkdir(parents=True, exist_ok=True)

    # Load AOI
    aoi = gpd.read_file(aoi_path)
    aoi_bounds = aoi.total_bounds  # minx, miny, maxx, maxy

    for product in products:
        product_dir = base_output_dir / product
        product_dir.mkdir(parents=True, exist_ok=True)

        items_to_extract = items_dict.get(product, [])
        granule_list = []  # Placeholder: list of URLs for GEDI granules

        logger.info(f"Fetching {product} granules within timeframe {timeframe['start']} to {timeframe['end']}")

        for granule_url in granule_list:
            granule_name = Path(granule_url).name
            granule_path = product_dir / granule_name
            download_gedi_granule(granule_url, granule_path)

            df = read_gedi_hdf5(granule_path, items_to_extract)

            # Filter by timeframe if timestamp exists
            if "shot_number" in df.columns and "sensing_time" in df.columns:
                df["sensing_time"] = pd.to_datetime(df["sensing_time"])
                df = df[
                    (df["sensing_time"] >= pd.to_datetime(timeframe["start"])) &
                    (df["sensing_time"] <= pd.to_datetime(timeframe["end"]))
                ]

            # Filter by AOI if lat/lon exist
            if set(["latitude_bin0", "longitude_bin0"]).issubset(df.columns):
                df = df[
                    (df["longitude_bin0"] >= aoi_bounds[0]) & 
                    (df["longitude_bin0"] <= aoi_bounds[2]) &
                    (df["latitude_bin0"] >= aoi_bounds[1]) &
                    (df["latitude_bin0"] <= aoi_bounds[3])
                ]

            # Save filtered granule as Parquet
            out_file = product_dir / f"{granule_name.replace('.h5','.parquet')}"
            df.to_parquet(out_file, index=False)
            logger.info(f"Saved filtered GEDI data to {out_file}")

    logger.info("GEDI fetch completed.")


if __name__ == "__main__":
    dummy_cfg = {
        "geography": "data/test_aoi.geojson",
        "eo": {
            "gedi": {
                "products": ["GEDI_L1B", "GEDI_L2A", "GEDI_L2B"],
                "items_to_extract": {
                    "GEDI_L1B": ["/geolocation/latitude_bin0","/geolocation/longitude_bin0","/shot_number","/geolocation/elevation_bin0"],
                    "GEDI_L2A": ["/shot_number","/quality_flag","/elev_lowestmode","/lat_lowestmode","/lon_lowestmode","/rh"],
                    "GEDI_L2B": ["geolocation/lat_lowestmode","geolocation/lon_lowestmode","rh100","pai","fhd_normal"]
                },
                "timeframe": {"start": "2019-01-01", "end": "2019-12-31"}
            }
        },
        "output_dir": "data/raw/eo/gedi"
    }
    fetch_gedi(dummy_cfg)
