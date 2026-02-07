# Raster-threading

[![CI](https://github.com/RichardScottOZ/Raster-threading/workflows/CI/badge.svg)](https://github.com/RichardScottOZ/Raster-threading/actions)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

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

Run tests with coverage:
```bash
pytest test_raster_threading.py --cov=raster_threading --cov-report=term
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

## Features

- ✅ Thread-safe GDAL operations using per-call dataset handles
- ✅ Support for GeoTIFF and ERS raster formats
- ✅ Comprehensive test suite covering concurrent reads, writes, and mixed operations
- ✅ Stress tests with 8 and 16 concurrent threads
- ✅ Multi-band raster support
- ✅ Continuous Integration via GitHub Actions

## Installation

### As a Library

```bash
pip install git+https://github.com/RichardScottOZ/Raster-threading.git
```

### For Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed setup instructions.

## Usage

```python
from raster_threading import create_raster, read_raster, write_raster_band
import numpy as np

# Create a new GeoTIFF
path = "output.tif"
create_raster(path, fmt="GTiff", width=100, height=100, fill_value=0.0)

# Read raster data
data = read_raster(path)

# Write new data to a band
new_data = np.random.rand(100, 100).astype(np.float32)
write_raster_band(path, new_data, band_index=1)
```

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
