#!/usr/bin/env python3
"""
Startup script for the Interactive Weather Dashboard
"""

import sys
import os
import argparse
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent.absolute()
sys.path.insert(0, str(project_root))

def check_requirements():
    """Check if required packages are installed"""
    required_packages = [
        'dash', 'plotly', 'pandas', 'numpy', 'xarray', 
        'duckdb', 'netcdf4', 'scipy'
    ]
    
    missing_packages = []
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        print("Missing required packages:")
        for package in missing_packages:
            print(f"  - {package}")
        print("\nInstall missing packages with:")
        print(f"pip install {' '.join(missing_packages)}")
        return False
    
    return True

def check_data_availability():
    """Check if the GFS Zarr store is available."""
    print("Checking data availability...")
    
    try:
        from config import ZARR_STORE_PATH as ZARR_PATH_REL
        zarr_store_path = project_root / ZARR_PATH_REL
    except ImportError:
        zarr_store_path = project_root / "data/processed/gfs_data.zarr"

    if os.path.exists(zarr_store_path):
        print(f"✓ GFS Zarr data store found at: {zarr_store_path}")
        return True
    else:
        print("\nWARNING: Processed GFS Zarr store not found!")
        print(f"Expected at: {zarr_store_path}")
        print("You may need to run the full data pipeline first.")
        print("Example: ./scr/run_pipeline.sh")
        return False

def run_dashboard(host='0.0.0.0', port=8050, debug=True):
    """Run the dashboard."""
    try:
        from visualization.interactive_dashboard import app
        
        print("\nStarting Weather Dashboard...")
        print(f"Dashboard will be available at: http://{host}:{port}")
        print("Press Ctrl+C to stop the dashboard")
        
        app.run_server(host=host, port=port, debug=debug)
        
    except ImportError as e:
        print(f"\nError: Could not import the dashboard application.")
        print(f"Details: {e}")
        print("Please ensure 'visualization/interactive_dashboard.py' exists and is correct.")
        sys.exit(1)
    except Exception as e:
        print(f"\nError starting dashboard: {e}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Start the Interactive Weather Dashboard")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8050, help="Port to bind to (default: 8050)")
    parser.add_argument("--no-debug", action="store_true", help="Disable debug mode")
    parser.add_argument("--check-only", action="store_true", help="Only check data availability, then exit")
    
    args = parser.parse_args()
    
    print("=" * 50)
    print("Interactive Weather Dashboard Startup")
    print("=" * 50)
    
    if not check_requirements():
        sys.exit(1)
    
    print("✓ All required packages are installed.")
    
    data_available = check_data_availability()
    
    if args.check_only:
        if data_available:
            print("\n✓ System is ready to run the dashboard.")
            sys.exit(0)
        else:
            print("\n✗ System is not ready - missing data.")
            sys.exit(1)
    
    if not data_available:
        response = input("\nProcessed data not found. The dashboard may not work correctly. Continue anyway? (y/N): ")
        if response.lower() != 'y':
            print("Exiting.")
            sys.exit(1)
    
    # Run the dashboard
    debug_mode = not args.no_debug
    run_dashboard(host=args.host, port=args.port, debug=debug_mode)

if __name__ == "__main__":
    main()
