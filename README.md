# Raster-threading

Python test harness that exercises GDAL reading and writing in a multi-threaded
fashion for both GeoTIFF and ERS rasters.

## Prerequisites

- Python 3
- GDAL with Python bindings (e.g. `sudo apt-get install gdal-bin libgdal-dev python3-gdal`)
- NumPy (installed alongside `python3-gdal` on most systems)

## Usage

Run the harness to generate synthetic data, write GeoTIFF and ERS grids in
parallel, and read them back concurrently:

```bash
python raster_threading_harness.py --output-dir /tmp/raster-artifacts --threads 4 --size 256
```

Outputs are written to the chosen directory:
- `threaded_geotiff.tif`
- `threaded_grid.ers`

Use `--help` to see adjustable parameters for thread count and raster size.
