# Contributing to Raster-threading

Thank you for your interest in contributing to Raster-threading! This document provides guidelines for contributing to the project.

## Development Setup

### System Dependencies

First, install GDAL system libraries:

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install libgdal-dev gdal-bin
```

**macOS (using Homebrew):**
```bash
brew install gdal
```

**Windows:**
Install GDAL using OSGeo4W or conda:
```bash
conda install -c conda-forge gdal
```

### Python Environment

1. Clone the repository:
```bash
git clone https://github.com/RichardScottOZ/Raster-threading.git
cd Raster-threading
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Install development dependencies:
```bash
pip install pytest pytest-cov
```

## Running Tests

Run all tests:
```bash
pytest test_raster_threading.py -v
```

Run tests with coverage:
```bash
pytest test_raster_threading.py --cov=raster_threading --cov-report=term
```

Run specific test classes:
```bash
pytest test_raster_threading.py::TestSingleThread -v
pytest test_raster_threading.py::TestConcurrentReads -v
pytest test_raster_threading.py::TestStress -v
```

## Code Style

This project follows PEP 8 style guidelines. Please ensure your code:
- Uses meaningful variable and function names
- Includes docstrings for all public functions
- Has proper type hints where applicable
- Maintains the existing code structure

## Submitting Changes

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Make your changes
4. Add or update tests as needed
5. Ensure all tests pass
6. Commit your changes (`git commit -m 'Add some feature'`)
7. Push to the branch (`git push origin feature/your-feature`)
8. Open a Pull Request

## Pull Request Guidelines

- Provide a clear description of the changes
- Reference any related issues
- Ensure CI tests pass
- Keep changes focused and atomic
- Update documentation if needed

## Reporting Issues

When reporting issues, please include:
- Python version
- GDAL version
- Operating system
- Steps to reproduce the issue
- Expected vs actual behavior
- Error messages or stack traces

## Testing Philosophy

This project emphasizes thread-safe GDAL operations. When adding features:
- Test single-threaded behavior first
- Add concurrent tests for thread safety
- Include stress tests with higher thread counts
- Test both GeoTIFF and ERS formats

## Questions?

Feel free to open an issue for questions or discussion!
