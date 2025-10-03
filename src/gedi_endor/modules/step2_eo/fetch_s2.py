# modules/step2_eo/fetch_s2.py

import logging
from pathlib import Path
import geopandas as gpd
import xarray as xr
import rioxarray
import numpy as np
from sentinelhub import SHConfig, BBox, CRS, SentinelHubRequest, DataCollection, bbox_to_dimensions

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def fetch_s2(cfg):
    """
    Fetch Sentinel-2 L2A Surface Reflectance imagery clipped to AOI and timeframe using Sentinel Hub API.
    
    Args:
        cfg: dict-like configuration containing:
            - geography: path to AOI GeoJSON/Shapefile
            - s2:
                - products: list of Sentinel-2 SR bands to fetch
                - timeframe: dict with 'start' and 'end' dates
            - output_dir: base directory to save raw EO data
    """
    logger.info("Starting Sentinel-2 fetch using Sentinel Hub API...")

    aoi_path = cfg["geography"]
    timeframe = cfg["s2"]["timeframe"]
    products = cfg["s2"]["products"]
    output_dir = Path(cfg["output_dir"]) / "s2"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load AOI
    aoi = gpd.read_file(aoi_path)
    aoi = aoi.to_crs(epsg=4326)
    bbox = BBox(bbox=aoi.total_bounds, crs=CRS.WGS84)

    # Sentinel Hub config (make sure your client ID & secret are set)
    config = SHConfig()
    if not config.sh_client_id or not config.sh_client_secret:
        logger.warning("Sentinel Hub credentials are not set in environment or config!")

    # Compute pixel size and output shape
    resolution = 10  # 10m
    size = bbox_to_dimensions(bbox, resolution=resolution)

    # Build SentinelHubRequest
    evalscript = f"""
        //VERSION=3
        function setup() {{
            return {{
                input: [{', '.join([f'{{band:"{b}"}}' for b in products])}],
                output: {{ bands: {len(products)}, sampleType: "FLOAT32" }}
            }};
        }}
        function evaluatePixel(sample) {{
            return [{', '.join([f'sample.{b}' for b in products])}];
        }}
    """

    request = SentinelHubRequest(
        evalscript=evalscript,
        input_data=[SentinelHubRequest.input_data(DataCollection.SENTINEL2_L2A,
                                                  time_interval=(timeframe['start'], timeframe['end']))],
        responses=[SentinelHubRequest.output_response('default', MimeType.TIFF)],
        bbox=bbox,
        size=size,
        config=config
    )

    # Execute request
    logger.info("Querying Sentinel Hub API for S2 imagery...")
    data = request.get_data(save_data=False)

    # Convert to xarray
    data_array = np.stack([d for d in data], axis=0)  # time, y, x, band
    ds = xr.DataArray(
        data_array,
        dims=("time", "y", "x", "band"),
        coords={"time": np.arange(len(data)), "band": products}
    ).to_dataset(name="S2_SR")
    ds.rio.write_crs("EPSG:4326", inplace=True)

    # Clip to AOI
    ds_clipped = ds.rio.clip(aoi.geometry, aoi.crs, drop=True)

    # Save as Zarr
    zarr_file = output_dir / f"s2_{timeframe['start']}_{timeframe['end']}.zarr"
    ds_clipped.to_zarr(zarr_file, mode="w")
    logger.info(f"Saved Sentinel-2 data to {zarr_file}")

    logger.info("Sentinel-2 fetch completed.")

if __name__ == "__main__":
    dummy_cfg = {
        "geography": "data/test_aoi.geojson",
        "s2": {
            "products": ["B2", "B3", "B4", "B8", "B11"],  # SR bands
            "timeframe": {"start": "2019-01-01", "end": "2019-12-31"}
        },
        "output_dir": "data/raw/eo"
    }
    run(dummy_cfg)
