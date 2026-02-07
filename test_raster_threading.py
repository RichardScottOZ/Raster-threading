"""Test harness for multi-threaded GDAL raster read/write operations.

Exercises concurrent reads, concurrent writes, and mixed read/write
scenarios using both GeoTIFF (.tif) and ERS (.ers) formats.
"""

import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pytest
from osgeo import gdal

from raster_threading import (
    FORMATS,
    create_raster,
    read_raster,
    write_raster_band,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

FORMAT_IDS = list(FORMATS.keys())  # ["GTiff", "ERS"]

RASTER_WIDTH = 64
RASTER_HEIGHT = 64
NUM_THREADS = 4


@pytest.fixture(params=FORMAT_IDS)
def raster_fmt(request):
    """Parameterised fixture that yields each raster format name."""
    return request.param


@pytest.fixture()
def tmp_dir(tmp_path):
    """Return a temporary directory path (str) for test outputs."""
    return str(tmp_path)


def _raster_path(tmp_dir: str, fmt: str, tag: str = "") -> str:
    """Build a raster file path inside *tmp_dir*."""
    ext = FORMATS[fmt]["extension"]
    return os.path.join(tmp_dir, f"test_{tag}{ext}")


# ---------------------------------------------------------------------------
# Basic sanity tests (single-threaded)
# ---------------------------------------------------------------------------


class TestSingleThread:
    """Verify the helper functions work correctly in isolation."""

    def test_create_and_read(self, tmp_dir, raster_fmt):
        path = _raster_path(tmp_dir, raster_fmt, "basic")
        create_raster(path, fmt=raster_fmt, width=RASTER_WIDTH, height=RASTER_HEIGHT, fill_value=42.0)

        data = read_raster(path)
        assert data.shape == (RASTER_HEIGHT, RASTER_WIDTH)
        np.testing.assert_allclose(data, 42.0, atol=1e-5)

    def test_write_and_read_back(self, tmp_dir, raster_fmt):
        path = _raster_path(tmp_dir, raster_fmt, "write")
        create_raster(path, fmt=raster_fmt, width=RASTER_WIDTH, height=RASTER_HEIGHT)

        expected = np.arange(RASTER_WIDTH * RASTER_HEIGHT, dtype=np.float32).reshape(
            RASTER_HEIGHT, RASTER_WIDTH
        )
        write_raster_band(path, expected)
        actual = read_raster(path)
        np.testing.assert_array_equal(actual, expected)

    def test_multiband(self, tmp_dir, raster_fmt):
        path = _raster_path(tmp_dir, raster_fmt, "multi")
        create_raster(path, fmt=raster_fmt, width=RASTER_WIDTH, height=RASTER_HEIGHT, bands=3)

        for b in range(1, 4):
            arr = np.full((RASTER_HEIGHT, RASTER_WIDTH), float(b), dtype=np.float32)
            write_raster_band(path, arr, band_index=b)

        for b in range(1, 4):
            data = read_raster(path, band_index=b)
            np.testing.assert_allclose(data, float(b), atol=1e-5)


# ---------------------------------------------------------------------------
# Concurrent-read tests
# ---------------------------------------------------------------------------


class TestConcurrentReads:
    """Multiple threads reading the same raster simultaneously."""

    def test_concurrent_reads_same_file(self, tmp_dir, raster_fmt):
        path = _raster_path(tmp_dir, raster_fmt, "cread")
        fill = 7.0
        create_raster(path, fmt=raster_fmt, width=RASTER_WIDTH, height=RASTER_HEIGHT, fill_value=fill)

        results = {}
        with ThreadPoolExecutor(max_workers=NUM_THREADS) as pool:
            futures = {pool.submit(read_raster, path): i for i in range(NUM_THREADS)}
            for fut in as_completed(futures):
                idx = futures[fut]
                results[idx] = fut.result()

        assert len(results) == NUM_THREADS
        for arr in results.values():
            np.testing.assert_allclose(arr, fill, atol=1e-5)

    def test_concurrent_reads_different_files(self, tmp_dir, raster_fmt):
        paths = []
        for i in range(NUM_THREADS):
            p = _raster_path(tmp_dir, raster_fmt, f"crdiff_{i}")
            create_raster(p, fmt=raster_fmt, width=RASTER_WIDTH, height=RASTER_HEIGHT, fill_value=float(i))
            paths.append(p)

        results = {}
        with ThreadPoolExecutor(max_workers=NUM_THREADS) as pool:
            futures = {pool.submit(read_raster, p): i for i, p in enumerate(paths)}
            for fut in as_completed(futures):
                idx = futures[fut]
                results[idx] = fut.result()

        for i, arr in results.items():
            np.testing.assert_allclose(arr, float(i), atol=1e-5)


# ---------------------------------------------------------------------------
# Concurrent-write tests
# ---------------------------------------------------------------------------


class TestConcurrentWrites:
    """Multiple threads writing to separate raster files simultaneously."""

    def test_concurrent_writes_separate_files(self, tmp_dir, raster_fmt):
        paths = []
        for i in range(NUM_THREADS):
            p = _raster_path(tmp_dir, raster_fmt, f"cwrite_{i}")
            create_raster(p, fmt=raster_fmt, width=RASTER_WIDTH, height=RASTER_HEIGHT)
            paths.append(p)

        def _write(idx):
            arr = np.full((RASTER_HEIGHT, RASTER_WIDTH), float(idx + 1), dtype=np.float32)
            write_raster_band(paths[idx], arr)
            return idx

        with ThreadPoolExecutor(max_workers=NUM_THREADS) as pool:
            list(pool.map(_write, range(NUM_THREADS)))

        for i, p in enumerate(paths):
            data = read_raster(p)
            np.testing.assert_allclose(data, float(i + 1), atol=1e-5)

    def test_concurrent_create_and_write(self, tmp_dir, raster_fmt):
        """Each thread creates a new file and writes unique data."""

        def _create_and_write(idx):
            p = os.path.join(tmp_dir, f"cw_{idx}{FORMATS[raster_fmt]['extension']}")
            create_raster(p, fmt=raster_fmt, width=RASTER_WIDTH, height=RASTER_HEIGHT)
            arr = np.full((RASTER_HEIGHT, RASTER_WIDTH), float(idx * 10), dtype=np.float32)
            write_raster_band(p, arr)
            return p, idx

        results = {}
        with ThreadPoolExecutor(max_workers=NUM_THREADS) as pool:
            futures = [pool.submit(_create_and_write, i) for i in range(NUM_THREADS)]
            for fut in as_completed(futures):
                p, idx = fut.result()
                results[idx] = p

        for idx, p in results.items():
            data = read_raster(p)
            np.testing.assert_allclose(data, float(idx * 10), atol=1e-5)


# ---------------------------------------------------------------------------
# Mixed concurrent read/write tests
# ---------------------------------------------------------------------------


class TestMixedConcurrency:
    """Threads reading from one file while other threads write to different files."""

    def test_readers_and_writers(self, tmp_dir, raster_fmt):
        read_path = _raster_path(tmp_dir, raster_fmt, "mixed_r")
        create_raster(
            read_path, fmt=raster_fmt, width=RASTER_WIDTH, height=RASTER_HEIGHT, fill_value=99.0
        )

        write_paths = []
        for i in range(NUM_THREADS):
            p = _raster_path(tmp_dir, raster_fmt, f"mixed_w{i}")
            create_raster(p, fmt=raster_fmt, width=RASTER_WIDTH, height=RASTER_HEIGHT)
            write_paths.append(p)

        read_results = []
        write_ok = []

        def _do_read():
            return read_raster(read_path)

        def _do_write(idx):
            arr = np.full((RASTER_HEIGHT, RASTER_WIDTH), float(idx + 100), dtype=np.float32)
            write_raster_band(write_paths[idx], arr)
            return idx

        with ThreadPoolExecutor(max_workers=NUM_THREADS * 2) as pool:
            read_futures = [pool.submit(_do_read) for _ in range(NUM_THREADS)]
            write_futures = [pool.submit(_do_write, i) for i in range(NUM_THREADS)]

            for fut in as_completed(read_futures):
                read_results.append(fut.result())
            for fut in as_completed(write_futures):
                write_ok.append(fut.result())

        # Verify reads
        for arr in read_results:
            np.testing.assert_allclose(arr, 99.0, atol=1e-5)

        # Verify writes
        for i in range(NUM_THREADS):
            data = read_raster(write_paths[i])
            np.testing.assert_allclose(data, float(i + 100), atol=1e-5)


# ---------------------------------------------------------------------------
# Stress / higher-thread-count tests
# ---------------------------------------------------------------------------


class TestStress:
    """Higher concurrency to surface potential race conditions."""

    @pytest.mark.parametrize("num_workers", [8, 16])
    def test_many_concurrent_reads(self, tmp_dir, raster_fmt, num_workers):
        path = _raster_path(tmp_dir, raster_fmt, f"stress_r{num_workers}")
        create_raster(path, fmt=raster_fmt, width=RASTER_WIDTH, height=RASTER_HEIGHT, fill_value=3.14)

        with ThreadPoolExecutor(max_workers=num_workers) as pool:
            futures = [pool.submit(read_raster, path) for _ in range(num_workers)]
            for fut in as_completed(futures):
                np.testing.assert_allclose(fut.result(), 3.14, atol=1e-4)

    @pytest.mark.parametrize("num_workers", [8, 16])
    def test_many_concurrent_writes(self, tmp_dir, raster_fmt, num_workers):
        paths = []
        for i in range(num_workers):
            p = os.path.join(tmp_dir, f"stress_w{i}{FORMATS[raster_fmt]['extension']}")
            create_raster(p, fmt=raster_fmt, width=RASTER_WIDTH, height=RASTER_HEIGHT)
            paths.append(p)

        def _write(idx):
            arr = np.full((RASTER_HEIGHT, RASTER_WIDTH), float(idx), dtype=np.float32)
            write_raster_band(paths[idx], arr)

        with ThreadPoolExecutor(max_workers=num_workers) as pool:
            list(pool.map(_write, range(num_workers)))

        for i, p in enumerate(paths):
            data = read_raster(p)
            np.testing.assert_allclose(data, float(i), atol=1e-5)
