# modules/step2_eo/fetch_s1.py

import logging
from pathlib import Path
import geopandas as gpd
import pandas as pd
from asf_search import Search

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def fetch_s1(cfg):
    """
    Fetch Sentinel-1 L2_RTC-S1 scenes via ASF API, clipped to AOI and timeframe.
    
    Args:
        cfg: dict-like configuration containing:
            - geography: path to AOI GeoJSON/Shapefile
            - eo -> S1:
                - timeframe: dict with 'start' and 'end'
                - products: list of polarizations to filter (optional)
            - output_dir: base directory to save raw S1 data
    """
    logger.info("Starting Sentinel-1 fetch...")

    # AOI
    aoi_path = cfg["geography"]
    aoi = gpd.read_file(aoi_path)
    aoi_bounds = aoi.total_bounds  # minx, miny, maxx, maxy
    minx, miny, maxx, maxy = aoi_bounds

    s1_cfg = cfg["eo"]["S1"]
    timeframe = s1_cfg.get("timeframe", {"start": "2019-01-01", "end": "2019-12-31"})
    output_dir = Path(cfg["output_dir"]) / "S1"
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Searching Sentinel-1 L2_RTC-S1 scenes via ASF API...")

    # Create ASF Search object
    s = Search()
    s.platform = "Sentinel-1"
    s.product_type = "L2_RTC-S1"
    s.start_time = timeframe["start"]
    s.end_time = timeframe["end"]
    s.bbox = [miny, minx, maxy, maxx]  # [south, west, north, east]

    # Fetch results
    results = s.get_results()
    if not results:
        logger.warning("No Sentinel-1 scenes found for given AOI/timeframe.")
        return

    logger.info(f"Found {len(results)} Sentinel-1 scenes. Saving to CSV...")

    # Convert results to DataFrame
    df = pd.DataFrame(results)

    # Optionally filter by polarization if provided
    polarizations = s1_cfg.get("products", None)
    if polarizations:
        df = df[df["polarizations"].apply(lambda x: any(p in x for p in polarizations))]

    # Save results
    out_file = output_dir / f"s1_search_results_{timeframe['start']}_{timeframe['end']}.csv"
    df.to_csv(out_file, index=False)
    logger.info(f"Sentinel-1 search results saved to {out_file}")

if __name__ == "__main__":
    # Minimal test config
    dummy_cfg = {
        "geography": "data/test_aoi.geojson",
        "eo": {
            "S1": {
                "timeframe": {"start": "2019-01-01", "end": "2019-12-31"},
                "products": ["VV","VH"]
            }
        },
        "output_dir": "data/raw/eo"
    }
    fetch_s1(dummy_cfg)
