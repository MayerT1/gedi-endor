from modules.step1_gedi import fetch, filter, stage
from pathlib import Path

# Minimal dummy config
cfg = {
    "geography": "data/test_aoi.geojson",
    "gedi": {
        "products": ["GEDI02_B", "GEDI02_A"],
        "fields": ["rh98", "pai", "fhd_normal"],
        "filters": {"quality_flag": 1, "sensitivity": 0.9},
        "timeframe": {"start": "2019-01-01", "end": "2019-12-31"}
    },
    "input_dir": "data/raw/gedi",
    "output_dir": "data/processed/gedi",
    "stage_dir": "data/staged/gedi",  # separate staging folder
    "naming_convention": "{product}_{start}_{end}.parquet"
}

# Ensure directories exist
Path(cfg["input_dir"]).mkdir(parents=True, exist_ok=True)
Path(cfg["output_dir"]).mkdir(parents=True, exist_ok=True)
Path(cfg["stage_dir"]).mkdir(parents=True, exist_ok=True)

# Run Step 1
fetch.run(cfg)
filter.run(cfg)
stage.run(cfg)  # stage.py now uses cfg["stage_dir"] internally
