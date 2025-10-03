# modules/step2_eo/gedi_filter.py

import logging
from pathlib import Path
import pandas as pd
import zarr
import xarray as xr

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def filter_gedi_df(df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    """
    Apply user-defined filters to a GEDI DataFrame.

    Args:
        df: DataFrame containing GEDI observations.
        filters: Dict of column -> filter condition.
                 Example: {"quality_flag": lambda x: x==1, "sensitivity": lambda x: x>0.9}

    Returns:
        Filtered DataFrame.
    """
    for col, func in filters.items():
        if col in df.columns:
            df = df[func(df[col])]
        else:
            logger.warning(f"Column {col} not in DataFrame, skipping filter.")
    return df


def run(cfg):
    """
    Filter GEDI data according to user specifications.

    Args:
        cfg: dict-like configuration containing:
            - input_dir: path to raw GEDI data (Parquet/Zarr)
            - output_dir: path to save filtered GEDI data
            - product_filters: dict mapping product -> dict of column -> filter function
    """
    input_dir = Path(cfg["input_dir"])
    output_dir = Path(cfg["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    product_filters = cfg.get("product_filters", {})

    # Process each product
    for product_dir in input_dir.iterdir():
        if not product_dir.is_dir():
            continue

        product_name = product_dir.name
        filters = product_filters.get(product_name, {})

        output_product_dir = output_dir / product_name
        output_product_dir.mkdir(parents=True, exist_ok=True)

        # Process each file in product directory
        for file in product_dir.glob("*.parquet"):
            logger.info(f"Filtering {file.name}")
            df = pd.read_parquet(file)
            df_filtered = filter_gedi_df(df, filters)

            # Save filtered output
            out_file = output_product_dir / file.name
            df_filtered.to_parquet(out_file, index=False)
            logger.info(f"Saved filtered data to {out_file}")


if __name__ == "__main__":
    # Example usage
    cfg = {
        "input_dir": "data/raw/eo/gedi",
        "output_dir": "data/processed/eo/gedi",
        "product_filters": {
            "GEDI_L2A": {
                "quality_flag": lambda x: x == 1,
                "sensitivity": lambda x: x > 0.9,
                "stale_return_flag": lambda x: x == 0
            },
            "GEDI_L2B": {
                "l2b_quality_flag": lambda x: x == 1,
                "pai": lambda x: x >= 0,
                "fhd_normal": lambda x: x >= 0
            }
        }
    }
    run(cfg)
