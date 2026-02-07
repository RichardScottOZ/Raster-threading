#!/usr/bin/env python3
"""
Threaded GDAL read/write harness that exercises GeoTIFF and ERS rasters.
Generates a synthetic grid, writes both formats in parallel, then reads them
back concurrently to validate access patterns.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import argparse

import numpy as np
from osgeo import gdal


def synthetic_grid(size: int) -> np.ndarray:
    """Build a deterministic grid so reads can be sanity-checked."""
    y_idx, x_idx = np.mgrid[:size, :size]
    data = np.sin(x_idx / 12.0) + np.cos(y_idx / 15.0)
    return data.astype(np.float32)


def write_raster(path: Path, driver_name: str, data: np.ndarray) -> Path:
    driver = gdal.GetDriverByName(driver_name)
    if driver is None:
        raise RuntimeError(f"GDAL driver {driver_name} is not available")

    height, width = data.shape
    dataset = driver.Create(str(path), width, height, 1, gdal.GDT_Float32)
    if dataset is None:
        raise RuntimeError(f"Could not create raster at {path}")

    dataset.SetGeoTransform((0, 1, 0, 0, 0, -1))
    dataset.SetProjection("")

    band = dataset.GetRasterBand(1)
    band.WriteArray(data)
    band.SetNoDataValue(-9999)
    band.FlushCache()
    dataset.FlushCache()
    dataset = None
    return path


def read_raster(path: Path) -> tuple[Path, float]:
    dataset = gdal.Open(str(path), gdal.GA_ReadOnly)
    if dataset is None:
        raise RuntimeError(f"Could not open raster at {path}")
    array = dataset.ReadAsArray()
    dataset = None
    return path, float(np.mean(array))


def threaded_write(out_dir: Path, data: np.ndarray, threads: int) -> list[Path]:
    targets = [
        ("GTiff", out_dir / "threaded_geotiff.tif"),
        ("ERS", out_dir / "threaded_grid.ers"),
    ]
    results: list[Path] = []

    with ThreadPoolExecutor(max_workers=threads) as executor:
        future_to_target = {
            executor.submit(write_raster, path, driver, data): (driver, path)
            for driver, path in targets
        }
        for future in as_completed(future_to_target):
            driver, path = future_to_target[future]
            results.append(path)
            print(f"Wrote {path.name} using {driver}")

    return results


def threaded_read(paths: list[Path], threads: int) -> list[tuple[Path, float]]:
    results: list[tuple[Path, float]] = []

    with ThreadPoolExecutor(max_workers=threads) as executor:
        future_map = {executor.submit(read_raster, path): path for path in paths}
        for future in as_completed(future_map):
            path, mean_val = future.result()
            results.append((path, mean_val))
            print(f"Read {path.name}: mean={mean_val:.4f}")

    return results


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Threaded GDAL read/write harness for GeoTIFF and ERS rasters"
    )
    parser.add_argument("--output-dir", default="artifacts", help="Directory to place generated rasters")
    parser.add_argument("--size", type=int, default=256, help="Raster width/height (square)")
    parser.add_argument("--threads", type=int, default=4, help="Thread count for reads/writes")
    args = parser.parse_args()

    gdal.UseExceptions()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    grid = synthetic_grid(args.size)
    print(f"Generating {args.size}x{args.size} grid and writing in {args.threads} threads...")
    written_paths = threaded_write(output_dir, grid, args.threads)

    print("Reading rasters in parallel...")
    stats = threaded_read(written_paths, args.threads)

    print("\nSummary:")
    for path, mean_val in stats:
        print(f"- {path}: mean={mean_val:.4f}")


if __name__ == "__main__":
    main()
