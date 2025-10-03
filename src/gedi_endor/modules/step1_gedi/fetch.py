# modules/step1_gedi/fetch.py

import os
import logging
from pathlib import Path
from harmony import Client  # assuming harmony-py is installed
from datetime import datetime

# Setup logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def run(cfg):
    """
    Fetch GEDI data for the specified AOI, products, fields, and timeframe.

    Args:
        cfg: OmegaConf / dict-like configuration containing:
            - geography: path to AOI (GeoJSON/shapefile)
            - gedi:
                - products: list of GEDI short names, e.g., ["GEDI02_B", "GEDI02_A"]
                - fields: list of fields to fetch, e.g., ["rh98", "pai", "fhd_normal"]
                - filters: dict of filter parameters (quality_flag, sensitivity, etc.)
                - timeframe: dict with 'start' and 'end' dates, e.g., {"start": "2019-01-01", "end": "2024-12-31"}
    Returns:
        None (saves raw GEDI data to raw/ folder)
    """
    logger.info("Starting GEDI fetch...")

    aoi_path = cfg["geography"]
    products = cfg["gedi"]["products"]
    fields = cfg["gedi"]["fields"]
    filters = cfg["gedi"]["filters"]
    timeframe = cfg["gedi"]["timeframe"]

    output_dir = Path("../../../data/raw/gedi")  # relative to module
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"AOI: {aoi_path}")
    logger.info(f"Products: {products}")
    logger.info(f"Fields: {fields}")
    logger.info(f"Filters: {filters}")
    logger.info(f"Timeframe: {timeframe}")
    logger.info(f"Saving raw GEDI data to: {output_dir.resolve()}")

    # Initialize Harmony client
    client = Client()

    for product in products:
        logger.info(f"Fetching GEDI product: {product}")
        # TODO: replace with actual Harmony API call
        # Example: result = client.query(product, aoi_path, start_date, end_date, fields)
        # Save to file
        filename = output_dir / f"{product}_{timeframe['start']}_{timeframe['end']}.parquet"
        # TODO: save 'result' to filename
        logger.info(f"Fetched data for {product} would be saved to {filename}")

    logger.info("GEDI fetch completed.")
