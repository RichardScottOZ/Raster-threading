"""
Core module for thread-safe GDAL raster operations.

This module provides thread-safe wrappers for GDAL operations, including
readers and writers for raster data, along with a thread manager to coordinate
multi-threaded operations.
"""

import threading
from typing import Optional, Tuple, List, Callable, Any
import numpy as np
from osgeo import gdal
from concurrent.futures import ThreadPoolExecutor, as_completed
import time


class ThreadSafeRasterReader:
    """Thread-safe raster reader using GDAL."""

    def __init__(self, filepath: str):
        """
        Initialize a thread-safe raster reader.

        Args:
            filepath: Path to the raster file
        """
        self.filepath = filepath
        self._lock = threading.Lock()
        self._dataset = None
        self._thread_local = threading.local()

    def open(self):
        """Open the raster dataset."""
        with self._lock:
            if self._dataset is None:
                self._dataset = gdal.Open(self.filepath, gdal.GA_ReadOnly)
                if self._dataset is None:
                    raise RuntimeError(f"Failed to open raster: {self.filepath}")

    def close(self):
        """Close the raster dataset."""
        with self._lock:
            if self._dataset is not None:
                self._dataset = None

    def get_metadata(self) -> dict:
        """
        Get raster metadata.

        Returns:
            Dictionary containing raster metadata
        """
        with self._lock:
            if self._dataset is None:
                raise RuntimeError("Dataset not opened")

            return {
                'width': self._dataset.RasterXSize,
                'height': self._dataset.RasterYSize,
                'bands': self._dataset.RasterCount,
                'projection': self._dataset.GetProjection(),
                'geotransform': self._dataset.GetGeoTransform(),
                'driver': self._dataset.GetDriver().ShortName
            }

    def read_block(self, band: int, x_offset: int, y_offset: int,
                   x_size: int, y_size: int) -> np.ndarray:
        """
        Read a block of data from a specific band.

        Args:
            band: Band number (1-indexed)
            x_offset: X offset in pixels
            y_offset: Y offset in pixels
            x_size: Width of block to read
            y_size: Height of block to read

        Returns:
            NumPy array containing the raster data
        """
        # Create thread-local dataset if needed
        if not hasattr(self._thread_local, 'dataset'):
            self._thread_local.dataset = gdal.Open(self.filepath, gdal.GA_ReadOnly)

        if self._thread_local.dataset is None:
            raise RuntimeError("Failed to open thread-local dataset")

        band_obj = self._thread_local.dataset.GetRasterBand(band)
        data = band_obj.ReadAsArray(x_offset, y_offset, x_size, y_size)
        return data

    def read_full_band(self, band: int) -> np.ndarray:
        """
        Read an entire band.

        Args:
            band: Band number (1-indexed)

        Returns:
            NumPy array containing the full band data
        """
        metadata = self.get_metadata()
        return self.read_block(band, 0, 0, metadata['width'], metadata['height'])


class ThreadSafeRasterWriter:
    """Thread-safe raster writer using GDAL."""

    def __init__(self, filepath: str, width: int, height: int, bands: int,
                 datatype: int = gdal.GDT_Float32, driver: str = 'GTiff',
                 projection: Optional[str] = None, geotransform: Optional[Tuple] = None):
        """
        Initialize a thread-safe raster writer.

        Args:
            filepath: Output file path
            width: Raster width in pixels
            height: Raster height in pixels
            bands: Number of bands
            datatype: GDAL data type (default: GDT_Float32)
            driver: GDAL driver name (default: 'GTiff')
            projection: Projection string (optional)
            geotransform: Geotransform tuple (optional)
        """
        self.filepath = filepath
        self.width = width
        self.height = height
        self.bands = bands
        self.datatype = datatype
        self.driver_name = driver
        self.projection = projection
        self.geotransform = geotransform
        self._lock = threading.Lock()
        self._dataset = None

    def create(self):
        """Create the output raster dataset."""
        with self._lock:
            if self._dataset is None:
                driver = gdal.GetDriverByName(self.driver_name)
                if driver is None:
                    raise RuntimeError(f"Failed to get driver: {self.driver_name}")

                # Create dataset
                self._dataset = driver.Create(
                    self.filepath, self.width, self.height, self.bands, self.datatype
                )

                if self._dataset is None:
                    raise RuntimeError(f"Failed to create raster: {self.filepath}")

                # Set projection and geotransform if provided
                if self.projection:
                    self._dataset.SetProjection(self.projection)
                if self.geotransform:
                    self._dataset.SetGeoTransform(self.geotransform)

    def write_block(self, band: int, x_offset: int, y_offset: int, data: np.ndarray):
        """
        Write a block of data to a specific band.

        Args:
            band: Band number (1-indexed)
            x_offset: X offset in pixels
            y_offset: Y offset in pixels
            data: NumPy array containing the data to write
        """
        with self._lock:
            if self._dataset is None:
                raise RuntimeError("Dataset not created")

            band_obj = self._dataset.GetRasterBand(band)
            band_obj.WriteArray(data, x_offset, y_offset)
            band_obj.FlushCache()

    def write_full_band(self, band: int, data: np.ndarray):
        """
        Write data to an entire band.

        Args:
            band: Band number (1-indexed)
            data: NumPy array containing the full band data
        """
        self.write_block(band, 0, 0, data)

    def close(self):
        """Close and finalize the raster dataset."""
        with self._lock:
            if self._dataset is not None:
                self._dataset.FlushCache()
                self._dataset = None


class RasterThreadManager:
    """
    Manager for coordinating multi-threaded raster operations.

    This class provides utilities for splitting rasters into blocks and
    processing them in parallel using multiple threads.
    """

    def __init__(self, max_workers: Optional[int] = None):
        """
        Initialize the thread manager.

        Args:
            max_workers: Maximum number of worker threads (default: CPU count)
        """
        self.max_workers = max_workers

    def split_into_blocks(self, width: int, height: int,
                         block_size: int = 256) -> List[Tuple[int, int, int, int]]:
        """
        Split a raster into blocks for parallel processing.

        Args:
            width: Raster width
            height: Raster height
            block_size: Size of each block in pixels

        Returns:
            List of tuples (x_offset, y_offset, x_size, y_size)
        """
        blocks = []
        for y in range(0, height, block_size):
            y_size = min(block_size, height - y)
            for x in range(0, width, block_size):
                x_size = min(block_size, width - x)
                blocks.append((x, y, x_size, y_size))
        return blocks

    def process_blocks_parallel(self, blocks: List[Tuple[int, int, int, int]],
                               process_func: Callable, *args, **kwargs) -> List[Any]:
        """
        Process raster blocks in parallel.

        Args:
            blocks: List of block tuples (x_offset, y_offset, x_size, y_size)
            process_func: Function to process each block
            *args: Additional positional arguments for process_func
            **kwargs: Additional keyword arguments for process_func

        Returns:
            List of results from processing each block
        """
        results = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(process_func, block, *args, **kwargs): block
                for block in blocks
            }

            for future in as_completed(futures):
                block = futures[future]
                try:
                    result = future.result()
                    results.append((block, result))
                except Exception as exc:
                    print(f"Block {block} generated an exception: {exc}")
                    results.append((block, None))

        return results

    def benchmark_operation(self, operation_func: Callable,
                          num_iterations: int = 1) -> dict:
        """
        Benchmark a raster operation.

        Args:
            operation_func: Function to benchmark
            num_iterations: Number of times to run the operation

        Returns:
            Dictionary containing timing statistics
        """
        times = []

        for _ in range(num_iterations):
            start_time = time.time()
            operation_func()
            end_time = time.time()
            times.append(end_time - start_time)

        return {
            'mean_time': np.mean(times),
            'std_time': np.std(times),
            'min_time': np.min(times),
            'max_time': np.max(times),
            'total_time': np.sum(times),
            'iterations': num_iterations
        }
