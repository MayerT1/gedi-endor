# modules/step2_eo/fetch_landsat.py

import logging
from pathlib import Path
import geopandas as gpd
import rioxarray
import xarray as xr
import requests
from shapely.geometry import mapping
import json
import os
from datetime import datetime
from urllib.parse import quote
import rasterio

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

EARTHDATA_USERNAME = os.getenv("EARTHDATA_USERNAME")
EARTHDATA_PASSWORD = os.getenv("EARTHDATA_PASSWORD")


def query_cmr(aoi_geom, start_date, end_date, collection_shortname="LANDSAT_8_C2_L2"):
    """
    Query NASA CMR for Landsat 8 Surface Reflectance granules over AOI and time frame.

    Args:
        aoi_geom: GeoJSON geometry (polygon)
        start_date: 'YYYY-MM-DD'
        end_date: 'YYYY-MM-DD'
        collection_shortname: Landsat 8 SR collection

    Returns:
        List of granule metadata dicts
    """
    logger.info(f"Querying CMR for {collection_shortname} from {start_date} to {end_date}...")
    
    cmr_url = "https://cmr.earthdata.nasa.gov/search/granules.json"
    polygon_wkt = mapping(aoi_geom)["coordinates"][0]
    polygon_str = " ".join([f"{x} {y}" for x, y in polygon_wkt])
    
    params = {
        "short_name": collection_shortname,
        "temporal": f"{start_date}T00:00:00Z,{end_date}T23:59:59Z",
        "polygon": polygon_str,
        "page_size": 2000,
        "sort_key": "-start_date",
    }
    
    response = requests.get(cmr_url, params=params, auth=(EARTHDATA_USERNAME, EARTHDATA_PASSWORD))
    response.raise_for_status()
    results = response.json()
    
    granules = results.get("feed", {}).get("entry", [])
    logger.info(f"Found {len(granules)} granules")
    return granules


def download_band(granule, band_name, output_dir):
    """
    Download a specific band of a granule and return as xarray.DataArray
    """
    for link in granule.get("links", []):
        if link.get("title", "").endswith(f"{band_name}.TIF") and "http" in link.get("href", ""):
            url = link["href"]
            local_path = Path(output_dir) / f"{granule['title']}_{band_name}.tif"
            if not local_path.exists():
                logger.info(f"Downloading {band_name} to {local_path}")
                r = requests.get(url, auth=(EARTHDATA_USERNAME, EARTHDATA_PASSWORD), stream=True)
                r.raise_for_status()
                with open(local_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        f.write(chunk)
            # Load with rioxarray
            arr = rioxarray.open_rasterio(local_path, masked=True)
            return arr
    return None


def fetch_landsat(cfg):
    """
    Fetch Landsat SR imagery clipped to AOI and time frame, saved as Zarr
    """
    logger.info("Starting Landsat fetch...")

    aoi_path = cfg["geography"]
    timeframe = cfg["landsat"]["timeframe"]
    products = cfg["landsat"]["products"]
    output_dir = Path(cfg["output_dir"]) / "landsat"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load AOI
    aoi = gpd.read_file(aoi_path)
    if len(aoi) != 1:
        logger.warning("Multiple geometries found, using first one.")
    geom = aoi.geometry.iloc[0]

    # Query CMR
    granules = query_cmr(geom, timeframe["start"], timeframe["end"])

    for granule in granules:
        scene_id = granule["title"]
        logger.info(f"Processing granule {scene_id}")
        da_list = []
        for band in products:
            da = download_band(granule, band, output_dir)
            if da is not None:
                # Clip to AOI
                da_clipped = da.rio.clip([geom], aoi.crs, drop=True)
                da_list.append(da_clipped)
            else:
                logger.warning(f"Band {band} not found for granule {scene_id}")

        if da_list:
            ds = xr.merge(da_list)
            zarr_path = output_dir / f"{scene_id}.zarr"
            ds.to_zarr(zarr_path, mode="w")
            logger.info(f"Saved granule to {zarr_path}")

    logger.info("Landsat fetch completed.")


if __name__ == "__main__":
    dummy_cfg = {
        "geography": "data/test_aoi.geojson",
        "landsat": {
            "products": ["SR_B2", "SR_B3", "SR_B4", "SR_B5", "SR_B6"],
            "timeframe": {"start": "2019-01-01", "end": "2019-12-31"}
        },
        "output_dir": "data/raw/eo"
    }
    run(dummy_cfg)
