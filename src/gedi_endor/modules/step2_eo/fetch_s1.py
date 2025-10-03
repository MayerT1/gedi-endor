# modules/step2_eo/fetch_s1.py

import logging
from pathlib import Path
import geopandas as gpd
import asf_search as asf
import rioxarray
import xarray as xr
from shapely.geometry import mapping

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def fetch_s1(cfg):
    """
    Fetch Sentinel-1 RTC scenes from ASF, clip to AOI, save as Zarr.

    Authentication:
        Uses Earthdata credentials stored in ~/.netrc (Linux/Mac)
        or C:\\Users\\<User>\\_netrc (Windows).
    """
    logger.info("Starting Sentinel-1 fetch → zarr...")

    # Load AOI
    aoi = gpd.read_file(cfg["geography"]).to_crs(epsg=4326)
    aoi_wkt = aoi.geometry.unary_union.wkt

    # Config
    s1_cfg = cfg["eo"]["S1"]
    timeframe = s1_cfg.get("timeframe", {})
    start = timeframe.get("start", "2019-01-01")
    end = timeframe.get("end", "2019-12-31")

    output_dir = Path(cfg["output_dir"]) / "S1"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Search ASF
    logger.info(f"Searching ASF Sentinel-1 RTC scenes from {start} to {end} ...")
    results = asf.geo_search(
        intersectsWith=aoi_wkt,
        platform=asf.PLATFORM.SENTINEL1,
        processingLevel="RTC/GRD",  # adjust if needed (e.g. "RTC")
        start=start,
        end=end,
        maxResults=500,
    )

    if not results:
        logger.warning("No Sentinel-1 scenes found for given AOI/timeframe.")
        return

    logger.info(f"Found {len(results)} candidate scenes")

    # Auth with .netrc automatically
    session = asf.ASFSession().auth_with_creds()

    ds_list = []

    for rec in results:
        try:
            # Download file
            rec.download(path=str(output_dir), session=session)

            # Determine local file path
            local_path = output_dir / rec.properties.get("fileName", "")
            if not local_path.exists():
                logger.warning(f"Could not find downloaded file for {rec}. Skipping.")
                continue

            # Open raster
            xr_ds = rioxarray.open_rasterio(local_path)

            # Clip to AOI
            clipped = xr_ds.rio.clip(aoi.geometry.apply(mapping), crs="EPSG:4326")

            # Add time dimension
            acq_time = rec.properties.get("startTime")
            clipped = clipped.expand_dims(time=[acq_time])

            ds_list.append(clipped)

        except Exception as e:
            logger.warning(f"Failed to process {rec}: {e}")
            continue

    if not ds_list:
        logger.warning("No datasets processed successfully.")
        return

    # Combine scenes along time dimension
    combined = xr.concat(ds_list, dim="time")

    # Save as Zarr
    zarr_path = output_dir / f"s1_timeseries_{start}_{end}.zarr"
    logger.info(f"Saving Zarr dataset to {zarr_path}")
    combined.to_zarr(zarr_path, mode="w")

    logger.info("Sentinel-1 fetch complete.")
