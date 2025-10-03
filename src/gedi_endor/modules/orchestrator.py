# modules/orchestrator.py

import logging
from pathlib import Path
from datetime import datetime

# Step 2 EO modules
from step2_eo.fetch import run as fetch_eo
from step2_eo.compute import run as compute_eo
from step2_eo.gedi_filter import run as filter_gedi

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def orchestrator(cfg):
    """
    Main pipeline orchestrator for Step 2 EO data.
    This handles fetching, filtering (GEDI), and computing indices
    for all EO products (S1, S2, Landsat, PACE, EMIT, DEM, GEDI).

    Args:
        cfg: dict-like configuration containing:
            - geography: path to AOI (GeoJSON/Shapefile)
            - eo: dict containing:
                - sources: list of EO sources ["S1","S2","Landsat","PACE","EMIT","DEM","GEDI"]
                - products: dict mapping source -> list of products
                - timeframe: dict with 'start' and 'end' ISO dates
                - composites: list of temporal composites, e.g., ["median","mean"]
                - phenology_windows: list of (start, end) tuples for temporal windows
                - gedi_filters: dict with GEDI product-specific filter criteria
            - input_dir: path to raw EO data
            - output_dir: path to processed EO outputs
    """

    logger.info("Starting Step 2 EO orchestrator...")

    # -----------------------------
    # Step 2a: Fetch EO datasets
    # -----------------------------
    logger.info("Fetching EO datasets...")
    fetch_eo(cfg)

    # -----------------------------
    # Step 2b: Filter GEDI immediately after fetch
    # -----------------------------
    if "GEDI" in cfg["eo"]["sources"]:
        logger.info("Filtering GEDI products...")
        filter_gedi(cfg)

    # -----------------------------
    # Step 2c: Compute indices & composites
    # -----------------------------
    logger.info("Computing EO indices and temporal composites...")
    compute_eo(cfg)

    logger.info("Step 2 EO processing completed.")


if __name__ == "__main__":
    # Example minimal configuration
    dummy_cfg = {
        "geography": "data/test_aoi.geojson",
        "eo": {
            "sources": ["GEDI","S1","S2","Landsat","PACE","EMIT","DEM"],
            "products": {
                "GEDI": ["GEDI_L2A", "GEDI_L2B"],
                "S1": ["VV","VH"],
                "S2": ["B4","B3","B8","B11"],
                "Landsat": ["SR_B2","SR_B3","SR_B4","SR_B5","SR_B6"],
                "PACE": ["pGreen1","pRed","p530","p570","p705","p800","p495","p550"],
                "EMIT": ["reflectance"],
                "DEM": ["elevation"]
            },
            "timeframe": {"start": "2019-01-01","end": "2019-12-31"},
            "composites": ["median","mean"],
            "phenology_windows": [("2019-04-01","2019-06-30"),("2019-07-01","2019-09-30")],
            "gedi_filters": {
                "GEDI_L2A": {"quality_flag": [0,1], "sensitivity": [0.9,1.0]},
                "GEDI_L2B": {"quality_flag": [0,1]}
            }
        },
        "input_dir": "data/raw/eo",
        "output_dir": "data/processed/eo"
    }

    orchestrator(dummy_cfg)
