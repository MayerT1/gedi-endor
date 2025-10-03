# modules/step2_eo/fetch.py

from .fetch_s1 import fetch_s1
from .fetch_s2 import fetch_s2
from .fetch_landsat import fetch_landsat
from .fetch_gedi import fetch_gedi
from .fetch_pace import fetch_pace
from .fetch_emit import fetch_emit
from .fetch_dem import fetch_dem
from .gedi_filter import run as filter_gedi

def run(cfg):
    """
    Unified EO fetch orchestrator. Calls individual fetch scripts per source.
    Applies GEDI filters automatically after fetch.
    """
    sources = cfg["eo"]["sources"]

    if "GEDI" in sources:
        fetch_gedi(cfg)
        # Apply filters right after fetch
        filter_gedi(cfg)

    if "S1" in sources:
        fetch_s1(cfg)
    if "S2" in sources:
        fetch_s2(cfg)
    if "Landsat" in sources:
        fetch_landsat(cfg)
    if "PACE" in sources:
        fetch_pace(cfg)
    if "EMIT" in sources:
        fetch_emit(cfg)
    if "DEM" in sources:
        fetch_dem(cfg)
