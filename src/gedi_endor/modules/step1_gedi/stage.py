# modules/step1_gedi/stage.py

import logging
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def run(cfg):
    """
    Stage GEDI data: select fields and save ready for downstream processing.

    Args:
        cfg: dict-like configuration containing:
            - input_dir: path to filtered GEDI data
            - stage_dir: path to save staged GEDI data
            - gedi.fields: list of fields to retain
            - naming_convention: string template for output filenames
    Returns:
        None (saves staged GEDI data)
    """
    logger.info("Starting GEDI staging...")

    input_dir = Path(cfg["input_dir"])
    output_dir = Path(cfg["stage_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    fields_to_retain = cfg["gedi"]["fields"]
    naming_template = cfg.get("naming_convention", "{product}.parquet")

    logger.info(f"Input directory: {input_dir.resolve()}")
    logger.info(f"Output (staged) directory: {output_dir.resolve()}")
    logger.info(f"Fields to retain: {fields_to_retain}")

    for file in input_dir.glob("*.parquet"):
        logger.info(f"Processing file: {file.name}")
        df = pd.read_parquet(file)

        # Keep only the requested fields if they exist in the dataframe
        df_staged = df[[col for col in fields_to_retain if col in df.columns]]

        # Build output filename
        product_name = file.stem.split("_")[0]
        start_date = cfg["gedi"]["timeframe"]["start"]
        end_date = cfg["gedi"]["timeframe"]["end"]
        output_file = output_dir / naming_template.format(product=product_name, start=start_date, end=end_date)

        df_staged.to_parquet(output_file)
        logger.info(f"Staged data saved to {output_file}")

    logger.info("GEDI staging completed.")
