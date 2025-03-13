#!/usr/bin/env python3
"""
Test script for gen Python bindings.
"""
import os
import sys
import importlib
import inspect

def check_module_path(module_name):
    """Check a module's file path if it can be imported."""
    try:
        module = importlib.import_module(module_name)
        print(f"✅ Successfully imported {module_name}")
        
        # Check the module's file path
        if hasattr(module, '__file__'):
            print(f"    Module file: {module.__file__}")
        else:
            print(f"  ❌ Module {module_name} has no __file__ attribute")
            
        # Print module attributes and documentation
        print(f"  📝 Module attributes: {sorted([attr for attr in dir(module) if not attr.startswith('_')])}")
        if module.__doc__:
            print(f"    Module docstring: {module.__doc__.strip()}")
        
        return module
    except ImportError as e:
        print(f"❌ Failed to import {module_name}: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error importing {module_name}: {type(e).__name__}: {e}")
        sys.exit(1)

def check_site_packages():
    """Check site-packages directories for gen files."""
    print("\n  Checking site-packages directories:")
    for path in sys.path:
        if 'site-packages' in path and os.path.isdir(path):
            gen_dir = os.path.join(path, 'gen')
            if os.path.isdir(gen_dir):
                print(f"  Found gen package at: {gen_dir}")
                print(f"  Files in directory: {sorted(os.listdir(gen_dir))}")
                
                # Look for .so files (compiled Rust extensions)
                so_files = [f for f in os.listdir(gen_dir) 
                           if f.endswith('.so') or f.endswith('.pyd') or f.endswith('.dylib')]
                if so_files:
                    print(f"  Found shared libraries: {so_files}")
                else:
                    print(f"  No shared library files found in {gen_dir}")

def check_package_metadata():
    """Check package metadata from the distribution."""
    try:
        from importlib.metadata import distribution, PackageNotFoundError
        try:
            dist = distribution('gen')
            print(f"\n Package metadata for gen:")
            print(f"  Version: {dist.version}")
            print(f"  Files: {sorted([f for f in dist.files if 'gen' in str(f)])}")
            print(f"  Entry points: {dist.entry_points}")
            print(f"  Requires: {dist.requires}")
        except PackageNotFoundError:
            print("❌ gen package metadata not found")
    except ImportError:
        # Fallback for Python < 3.8
        print("❌ importlib.metadata not available (Python < 3.8)")
        
def main():
    """Run the test script."""
    print(f"🔍 Python version: {sys.version}")
    print(f"🔍 Python executable: {sys.executable}")
    print(f"🔍 Current directory: {os.getcwd()}")
    
    # Check gen module
    gen_mod = check_module_path('gen')
    
    # Check gen.gen module
    if gen_mod:
        gen_impl = check_module_path('gen.gen')
        
        # Try to import specific classes from gen
        if hasattr(gen_mod, 'Repository'):
            print("\n🔍 Found Repository class in gen module")
            try:
                repo_class = gen_mod.Repository
                print(f"     Repository class attributes: {dir(repo_class)}")
                print(f"     Repository class docstring: {repo_class.__doc__}")
            except Exception as e:
                print(f"  ❌ Error examining Repository class: {e}")
    
    # Check where the package files are installed
    check_site_packages()
    
    # Check package metadata
    check_package_metadata()
    
    print("\n✅ Test script completed")

if __name__ == "__main__":
    main() 