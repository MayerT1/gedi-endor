# modules/step2_eo/compute.py

import logging
from pathlib import Path
import pandas as pd
import numpy as np
import xarray as xr
import rioxarray
from scipy.optimize import least_squares

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# -----------------------------
# EMIT Liquid Water (CWC/EWT)
# -----------------------------
def beer_lambert_model(x, y, wl, alpha_lw):
    attenuation = np.exp(-x[0] * 1e7 * alpha_lw)
    rho = (x[1] + x[2] * wl) * attenuation
    resid = rho - y
    return resid

def invert_liquid_water(rfl_meas, wl, abs_co_w, lw_init=(0.02,0.3,0.0002),
                        lw_bounds=([0,0.5],[0,1.0],[-0.0004,0.0004])):
    x_opt = least_squares(
        fun=beer_lambert_model,
        x0=lw_init,
        jac="2-point",
        method="trf",
        bounds=(np.array([lw_bounds[ii][0] for ii in range(3)]),
                np.array([lw_bounds[ii][1] for ii in range(3)])),
        max_nfev=15,
        args=(rfl_meas, wl, abs_co_w)
    )
    return x_opt.x

def compute_emit_cwc(df):
    if 'reflectance' not in df.columns:
        return df
    wl = np.arange(400, 2500, 5)
    abs_co_w = np.ones_like(wl) * 0.001  # Placeholder: replace with actual water absorption
    cwc, intercept, slope = invert_liquid_water(df['reflectance'].values, wl, abs_co_w)
    df['CWC'] = cwc
    df['EWT'] = cwc  # scale if needed
    logger.info("Computed EMIT CWC/EWT")
    return df

# -----------------------------
# PACE Indices
# -----------------------------
def compute_pace_indices(df):
    if set(['pGreen1','pRed']).issubset(df.columns):
        df['CCI'] = (df['pGreen1'] - df['pRed']) / (df['pGreen1'] + df['pRed'])
    if set(['p530','p570']).issubset(df.columns):
        df['PRI'] = (df['p530'] - df['p570']) / (df['p530'] + df['p570'])
    if set(['p800','p705']).issubset(df.columns):
        df['CIRE'] = (df['p800']/df['p705']) - 1
    if set(['p495','p705','p800']).issubset(df.columns):
        df['Car'] = ((1/df['p495'] - 1/df['p705']) * df['p800'])
    if set(['p550','p705','p800']).issubset(df.columns):
        df['mARI'] = ((1/df['p550'] - 1/df['p705']) * df['p800'])
    logger.info("Computed PACE indices")
    return df

# -----------------------------
# Sentinel-1 SAR ratios
# -----------------------------
def compute_s1_indices(df):
    if set(['VV','VH']).issubset(df.columns):
        df['VH_div_VV'] = df['VH'] / df['VV']
        df['VH_minus_VV'] = (df['VH'] - df['VV']) / (df['VH'] + df['VV'])
        logger.info("Computed Sentinel-1 SAR ratios")
    return df

# -----------------------------
# Sentinel-2 / Landsat indices
# -----------------------------
def compute_optical_indices(df):
    if set(['B4','B8']).issubset(df.columns):
        df['NDVI'] = (df['B8'] - df['B4']) / (df['B8'] + df['B4'])
    if set(['B3','B8']).issubset(df.columns):
        df['NDWI'] = (df['B3'] - df['B8']) / (df['B3'] + df['B8'])
    if set(['B3','B11']).issubset(df.columns):
        df['MNDWI'] = (df['B3'] - df['B11']) / (df['B3'] + df['B11'])
    if set(['B4','B8']).issubset(df.columns):
        df['SAVI'] = ((df['B8'] - df['B4']) / (df['B8'] + df['B4'] + 0.5)) * 1.5
    if set(['B8','B11']).issubset(df.columns):
        df['NDMI'] = (df['B8'] - df['B11']) / (df['B8'] + df['B11'])
    if set(['B11','B8']).issubset(df.columns):
        df['NDBI'] = (df['B11'] - df['B8']) / (df['B11'] + df['B8'])
    logger.info("Computed optical indices (S2/Landsat)")
    return df

# -----------------------------
# DEM indices
# -----------------------------
def compute_dem(df):
    if 'elevation' in df.columns:
        df['slope'] = np.gradient(df['elevation'])
        logger.info("Computed DEM slope")
    return df

# -----------------------------
# Temporal composites
# -----------------------------
def apply_temporal_composites(df, composites, phenology_windows):
    df["time"] = pd.to_datetime(df["time"])
    composite_dfs = []

    for win_start, win_end in phenology_windows:
        win_start = pd.to_datetime(win_start)
        win_end = pd.to_datetime(win_end)
        df_win = df[(df["time"] >= win_start) & (df["time"] <= win_end)]
        if df_win.empty:
            continue

        for comp in composites:
            if comp == "median":
                df_comp = df_win.groupby("time").median(numeric_only=True)
            elif comp == "mean":
                df_comp = df_win.groupby("time").mean(numeric_only=True)
            else:
                continue
            df_comp = df_comp.reset_index()
            df_comp.attrs["window_start"] = str(win_start)
            df_comp.attrs["window_end"] = str(win_end)
            df_comp.attrs["composite_type"] = comp
            composite_dfs.append(df_comp)

    return composite_dfs

# -----------------------------
# Compute orchestrator
# -----------------------------
def run(cfg):
    input_dir = Path(cfg["input_dir"])
    output_dir = Path(cfg["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    eo_cfg = cfg["eo"]
    sources = eo_cfg["sources"]
    composites = eo_cfg.get("composites", ["median"])
    phenology_windows = eo_cfg.get("phenology_windows", [])

    for source in sources:
        src_in_dir = input_dir / source.lower()
        src_out_dir = output_dir / source.lower()
        src_out_dir.mkdir(parents=True, exist_ok=True)

        for zarr_file in src_in_dir.glob("*.zarr"):
            logger.info(f"Processing {zarr_file.name}")
            ds = xr.open_zarr(zarr_file)
            df = ds.to_dataframe().reset_index()

            # Compute indices
            if source == "EMIT":
                df = compute_emit_cwc(df)
            elif source == "PACE":
                df = compute_pace_indices(df)
            elif source == "S1":
                df = compute_s1_indices(df)
            elif source in ["S2", "Landsat"]:
                df = compute_optical_indices(df)
            elif source == "DEM":
                df = compute_dem(df)

            # Temporal composites for non-GEDI/DEM
            if source not in ["GEDI","DEM"] and phenology_windows:
                composite_dfs = apply_temporal_composites(df, composites, phenology_windows)
                for df_comp in composite_dfs:
                    df_xr = df_comp.set_index("time").to_xarray()
                    window_start = df_comp.attrs["window_start"].replace("-", "")
                    window_end = df_comp.attrs["window_end"].replace("-", "")
                    comp_type = df_comp.attrs["composite_type"]
                    zarr_out = src_out_dir / f"{zarr_file.stem}_{comp_type}_{window_start}_{window_end}.zarr"
                    df_xr.to_zarr(zarr_out, mode="w")
                    logger.info(f"Saved {comp_type} composite for {window_start}-{window_end} to {zarr_out}")
            else:
                df_xr = df.set_index("time").to_xarray()
                zarr_out = src_out_dir / zarr_file.name
                df_xr.to_zarr(zarr_out, mode="w")
                logger.info(f"Saved computed features to {zarr_out}")

    logger.info("EO feature computation completed.")
