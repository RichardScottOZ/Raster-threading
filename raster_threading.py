"""Helpers for creating, reading, and writing GeoTIFF and ERS rasters via GDAL."""

from osgeo import gdal, osr
import numpy as np

gdal.UseExceptions()

# Supported format metadata
FORMATS = {
    "GTiff": {"extension": ".tif", "driver": "GTiff"},
    "ERS": {"extension": ".ers", "driver": "ERS"},
}


def _make_srs(epsg: int = 4326) -> str:
    """Return a WKT spatial reference for the given EPSG code."""
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(epsg)
    return srs.ExportToWkt()


def create_raster(
    path: str,
    *,
    fmt: str = "GTiff",
    width: int = 64,
    height: int = 64,
    bands: int = 1,
    dtype=gdal.GDT_Float32,
    fill_value: float = 0.0,
    epsg: int = 4326,
) -> str:
    """Create a new raster file filled with *fill_value* and return its path.

    Parameters
    ----------
    path : str
        Output file path.
    fmt : str
        GDAL driver short name (``GTiff`` or ``ERS``).
    width, height : int
        Pixel dimensions.
    bands : int
        Number of raster bands.
    dtype : int
        GDAL data type constant.
    fill_value : float
        Value used to initialise every pixel.
    epsg : int
        EPSG code for the coordinate reference system.
    """
    driver = gdal.GetDriverByName(fmt)
    if driver is None:
        raise RuntimeError(f"GDAL driver '{fmt}' is not available")
    ds = driver.Create(path, width, height, bands, dtype)
    ds.SetGeoTransform((0.0, 1.0, 0.0, 0.0, 0.0, -1.0))
    ds.SetProjection(_make_srs(epsg))
    for b in range(1, bands + 1):
        band = ds.GetRasterBand(b)
        band.Fill(fill_value)
        band.FlushCache()
    ds.FlushCache()
    ds = None  # close
    return path


def read_raster(path: str, band_index: int = 1) -> np.ndarray:
    """Open *path* read-only and return band data as a NumPy array.

    Each call opens and closes its own dataset handle so that it is safe to
    invoke from multiple threads simultaneously on the same or different files.
    """
    ds = gdal.Open(path, gdal.GA_ReadOnly)
    if ds is None:
        raise RuntimeError(f"Cannot open raster: {path}")
    band = ds.GetRasterBand(band_index)
    data = band.ReadAsArray()
    ds = None  # close
    return data


def write_raster_band(
    path: str,
    data: np.ndarray,
    band_index: int = 1,
) -> None:
    """Open *path* for update and overwrite one band with *data*.

    Each call opens and closes its own dataset handle.
    """
    ds = gdal.Open(path, gdal.GA_Update)
    if ds is None:
        raise RuntimeError(f"Cannot open raster for writing: {path}")
    band = ds.GetRasterBand(band_index)
    band.WriteArray(data)
    band.FlushCache()
    ds.FlushCache()
    ds = None  # close
