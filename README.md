# Raster-threading

Multi-threaded GDAL raster read/write test harness for GeoTIFF and ERS grids.

## Setup

```bash
# Install system GDAL (Ubuntu/Debian)
sudo apt-get install libgdal-dev gdal-bin

# Install Python dependencies
pip install -r requirements.txt
```

## Running Tests

```bash
pytest test_raster_threading.py -v
```

## Overview

- **`raster_threading.py`** – helper functions for creating, reading, and
  writing rasters via GDAL using per-call dataset handles (thread-safe
  pattern).
- **`test_raster_threading.py`** – pytest harness that exercises:
  - Single-threaded create / read / write / multi-band round-trips
  - Concurrent reads of the same file from multiple threads
  - Concurrent reads of different files
  - Concurrent writes to separate files
  - Mixed concurrent readers and writers
  - Stress tests with 8 and 16 threads

All scenarios run against both **GeoTIFF** (`.tif`) and **ERS** (`.ers`)
formats via parameterised fixtures.
