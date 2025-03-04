"""Python bindings to the Gen version control system."""

__version__ = "0.1.0"

# Bindings can come through a Python intermediate layer (helpers.py) or the compiled Rust library itself

try:
    # Through Python
    from .helpers import GraphLayout

    # Directly from Rust
    from .gen import Database, Accession, BaseLayout, ScaledLayout, connect, get_accessions
    
    # Make those classes and functions available at the package level
    __all__ = ["Database", "Accession", "BaseLayout", "ScaledLayout", "connect", "get_accessions"]
    
except ImportError as e:
    import sys
    import warnings
    import os
    
    warnings.warn(f"Failed to import Gen modules: {e}")
    
    # Try to print diagnostic information to help with troubleshooting
    package_dir = os.path.dirname(__file__)
    warnings.warn(f"Package directory contents: {os.listdir(package_dir)}")
    
    __all__ = []
