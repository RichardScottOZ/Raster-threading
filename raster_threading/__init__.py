"""
Raster Threading - A test harness for multi-threaded GDAL raster operations.

This package provides utilities for testing and benchmarking multi-threaded
read and write operations on raster data using GDAL, supporting both GeoTIFF
and ERS grid formats.
"""

__version__ = "0.1.0"

from .core import RasterThreadManager, ThreadSafeRasterReader, ThreadSafeRasterWriter
from .test_harness import RasterTestHarness

__all__ = [
    "RasterThreadManager",
    "ThreadSafeRasterReader",
    "ThreadSafeRasterWriter",
    "RasterTestHarness",
]
