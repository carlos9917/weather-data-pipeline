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
    """Check if any processed Zarr data is available."""
    print("Checking data availability...")
    
    processed_dir = project_root / "data" / "processed"

    if not processed_dir.exists():
        print(f"\nWARNING: Processed data directory not found at {processed_dir}")
        print("You may need to run the data processing pipeline first.")
        return False

    # Check for any Zarr store in the directory
    zarr_stores = list(processed_dir.glob('*.zarr'))

    if zarr_stores:
        print(f"✓ Found {len(zarr_stores)} processed Zarr data store(s) in: {processed_dir}")
        return True
    else:
        print("\nWARNING: No processed Zarr stores found!")
        print(f"Expected data in the format 'gfs_YYYYMMDD_CC.zarr' or 'met_data_YYYYMMDD_CC.zarr' in {processed_dir}")
        print("You may need to run the full data pipeline first.")
        print("Example: ./scr/run_pipeline.sh")
        return False


def run_dashboard(host='0.0.0.0', port=8050, debug=True,dashboard_type="simple"):
    """Run the dashboard."""
    try:
        if dashboard_type == "simple":
            from visualization.interactive_dashboard import app
        elif dashboard_type == "scatter":
            from visualization.interactive_dashboard_scatter import app
        
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
    parser.add_argument("--dashboard_type", type=str, default="simple",help="type of dashboard")
    
    args = parser.parse_args()
    
    print("=" * 50)
    print("Interactive Weather Dashboard Startup")
    print("=" * 50)
    
    #if not check_requirements():
    #    sys.exit(1)
    
    #print("✓ All required packages are installed.")
    
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
    run_dashboard(host=args.host, port=args.port, debug=debug_mode,dashboard_type=args.dashboard_type)

if __name__ == "__main__":
    main()
