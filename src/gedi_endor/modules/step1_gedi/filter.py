# modules/step1_gedi/filter.py

import logging
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def run(cfg):
    """
    Filter downloaded GEDI data based on quality flags, sensitivity, and other criteria.

    Args:
        cfg: dict-like configuration containing:
            - gedi:
                - fields: list of GEDI fields to use
                - filters: dict of filtering rules, e.g.,
                  {"sensitivity": 0.9, "quality_flag": 1}
            - input_dir: path to raw GEDI data
            - output_dir: path to save filtered GEDI data
    Returns:
        None (saves filtered GEDI data to processed folder)
    """
    logger.info("Starting GEDI filtering...")

    input_dir = Path(cfg["input_dir"])
    output_dir = Path(cfg["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    fields = cfg["gedi"]["fields"]
    filters = cfg["gedi"]["filters"]

    logger.info(f"Filtering parameters: {filters}")
    logger.info(f"Input directory: {input_dir.resolve()}")
    logger.info(f"Output directory: {output_dir.resolve()}")

    # Loop over all raw GEDI files
    for file in input_dir.glob("*.parquet"):
        logger.info(f"Processing file: {file.name}")
        df = pd.read_parquet(file)

        # Apply filters (placeholder - implement your actual logic here)
        # Example:
        # df_filtered = df[df['quality_flag'] <= filters['quality_flag']]
        # df_filtered = df_filtered[df_filtered['sensitivity'] >= filters['sensitivity']]
        df_filtered = df  # placeholder for now

        out_file = output_dir / file.name
        df_filtered.to_parquet(out_file)
        logger.info(f"Filtered data saved to {out_file}")

    logger.info("GEDI filtering completed.")
