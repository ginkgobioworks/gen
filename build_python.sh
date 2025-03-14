#!/bin/bash
set -e

# Check if .venv directory exists and activate it if not already in a virtualenv
if [ -z "$VIRTUAL_ENV" ] && [ -d ".venv" ]; then
  echo "Found .venv directory, activating virtual environment..."
  source .venv/bin/activate
  echo "Virtual environment activated: $VIRTUAL_ENV"
elif [ -z "$VIRTUAL_ENV" ]; then
  echo "Error: No active virtualenv detected and no .venv directory found."
  echo "Please activate a virtualenv or create a .venv directory before running this script."
  exit 1
fi

# Default: don't build wheel for distribution
BUILD_WHEEL=false

# Parse arguments
for arg in "$@"; do
  case $arg in
    --deploy|--with-wheel)
      BUILD_WHEEL=true
      shift
      ;;
    -h|--help)
      echo "Usage: $0 [OPTIONS]"
      echo "Options:"
      echo "  --deploy, --with-wheel  Also build wheel for distribution"
      echo "  -h, --help              Show this help message"
      exit 0
      ;;
  esac
done

# Print Python information
echo "Python information:"
echo $(python -c "import sys; print(sys.executable)")
echo $(python -c "import platform; print(f'Python {platform.python_version()}')")
echo "Python path: $(which python)"
echo "Python library path: $(python -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR"))')"
echo

# Print environment variables
echo "Environment variables:"
echo "PYTHONPATH=$PYTHONPATH"
echo "PYTHON_SYS_EXECUTABLE=$(python -c 'import sys; print(sys.executable)')"
echo

# Clean previous builds
echo "Cleaning previous builds..."
rm -rf target/wheels
# Uninstall if already installed
pip uninstall -y gen || true
echo

# Build the Python package
echo "Building Python package..."
cargo clean

# Use maturin develop for faster development builds
echo "Running maturin develop for development build..."
maturin develop --release --features python-bindings

# Note: everything below here is about building the wheel for distribution,
# todo: incorporate into build infrastructure

if [ "$BUILD_WHEEL" = true ]; then
  echo "Building wheel for distribution..."
  
  # Also build the wheel for distribution
  echo "Running maturin build for wheel creation..."
  maturin build --release --features python-bindings

  # Find the newest wheel
  WHEEL=$(ls -t target/wheels/*.whl | head -1)
  echo "Built wheel: $WHEEL"

  # Check the contents of the wheel
  echo "Checking wheel contents..."
  unzip -l $WHEEL | grep -E "\.so|\.dylib|\.pyd" || echo "No shared library found in wheel!"

  # Install the wheel
  echo "Installing package..."
  pip install $WHEEL

  # Verify installation
  echo "Verifying installation..."
  echo "Python paths:"
  python -c "import sys; print('\n  '.join([''] + sys.path))"
  echo

  echo "Installed packages:"
  pip list
  echo

else
  echo "Skipping wheel build for distribution. Use --deploy or --with-wheel to build the wheel."
fi 
