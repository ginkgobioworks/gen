#!/bin/bash
set -e

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

echo "Attempting to import gen:"
python -c '
import sys
try:
    import gen
    print(f"  Successfully imported gen {gen.__version__}")
    print(f"  Available attributes: {dir(gen)}")
    try:
        import gen.gen
        print(f"  Successfully imported gen.gen")
        print(f"  Available attributes: {dir(gen.gen)}")
    except ImportError as e:
        print(f"  Failed to import gen.gen: {e}")
        import os
        package_dir = os.path.dirname(gen.__file__)
        print(f"  Looking for .so files in {package_dir}:")
        print(f"    Files found: {os.listdir(package_dir)}")
except ImportError as e:
    print(f"Failed to import gen: {e}")
'

echo "Done! The package is now installed and ready to use."
echo "Example usage:"
echo 'python -c "
try:
    import gen
    print(\"Connection example:\")
    db = gen.Database(\"your_database.db\")
    print(\"Database connected successfully\")
except Exception as e:
    print(f\"Error: {e}\")
"' 