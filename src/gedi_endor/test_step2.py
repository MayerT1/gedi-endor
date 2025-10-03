# test_step2.py
import sys
from pathlib import Path
import logging

# Add your src folder to the Python path
sys.path.append(r"C:\Users\Mayer\Documents\GitHub\gedi-endor\src\gedi_endor\modules")

from orchestrator import orchestrator

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Configuration for Step 2 EO pipeline
cfg = {
    "geography": r"C:\Users\Mayer\Documents\GitHub\gedi-endor\src\gedi_endor\data\test_aoi.geojson",
    "eo": {
        "sources": ["GEDI", "S1", "S2", "Landsat", "PACE", "EMIT", "DEM"],
        "gedi": {
            "products": ["GEDI_L2A", "GEDI_L2B"],
            "items_to_extract": {
                "GEDI_L2A": ["/shot_number","/quality_flag","/elev_lowestmode","/lat_lowestmode","/lon_lowestmode","/rh"],
                "GEDI_L2B": ["geolocation/lat_lowestmode","geolocation/lon_lowestmode","rh100","pai","fhd_normal"]
            },
            "timeframe": {"start": "2019-01-01", "end": "2019-12-31"}
        },
        "S1": {
            "products": ["VV","VH"],
            "timeframe": {"start": "2019-01-01", "end": "2019-12-31"}
        },
        "S2": {
            "products": ["B2","B3","B4","B8","B11"],
            "timeframe": {"start": "2019-01-01", "end": "2019-12-31"}
        },
        "Landsat": {
            "products": ["SR_B2","SR_B3","SR_B4","SR_B5","SR_B6"],
            "timeframe": {"start": "2019-01-01", "end": "2019-12-31"}
        },
        "PACE": {
            "products": ["pGreen1","pRed","p530","p570","p705","p800","p495","p550"],
            "timeframe": {"start": "2019-01-01", "end": "2019-12-31"}
        },
        "EMIT": {
            "products": ["reflectance"],
            "timeframe": {"start": "2019-01-01", "end": "2019-12-31"}
        },
        "DEM": {
            "products": ["elevation"]
        },
        "composites": ["median", "mean"],
        "phenology_windows": [("2019-04-01","2019-06-30"), ("2019-07-01","2019-09-30")],
        "gedi_filters": {
            "GEDI_L2A": {"quality_flag": [0,1], "sensitivity": [0.9,1.0]},
            "GEDI_L2B": {"quality_flag": [0,1]}
        }
    },
    "input_dir": r"C:\Users\Mayer\Documents\GitHub\gedi-endor\src\gedi_endor\data\raw\eo",
    "output_dir": r"C:\Users\Mayer\Documents\GitHub\gedi-endor\src\gedi_endor\data\processed\eo"
}

if __name__ == "__main__":
    logging.info("Running Step 2 EO pipeline test...")
    
    # Ensure input/output directories exist
    Path(cfg["input_dir"]).mkdir(parents=True, exist_ok=True)
    Path(cfg["output_dir"]).mkdir(parents=True, exist_ok=True)
    
    orchestrator(cfg)
    
    logging.info("Step 2 EO pipeline test completed.")
