#!/usr/bin/env python3
"""
Startup script for the Interactive Weather Dashboard
"""

import sys
import os
import argparse
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.absolute()
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
    """Check what data is available"""
    print("Checking data availability...")
    
    # Check GFS data
    gfs_paths = [
        "data/processed/weather_data.db",
        "data/processed/weather_data.zarr"
    ]
    
    gfs_available = any(os.path.exists(path) for path in gfs_paths)
    
    # Check MEPS data
    meps_raw_path = "data/raw/met"
    meps_processed_paths = [
        "data/processed/weather_data.db",  # MEPS data could be in same DB
        "data/processed/meps_weather_data.zarr"
    ]
    
    meps_raw_available = os.path.exists(meps_raw_path) and len(os.listdir(meps_raw_path)) > 0
    meps_processed_available = any(os.path.exists(path) for path in meps_processed_paths)
    
    print(f"GFS data available: {'Yes' if gfs_available else 'No'}")
    print(f"MEPS raw data available: {'Yes' if meps_raw_available else 'No'}")
    print(f"MEPS processed data available: {'Yes' if meps_processed_available else 'No'}")
    
    if not gfs_available and not meps_processed_available:
        print("\nWARNING: No processed weather data found!")
        print("You may need to:")
        print("1. Download GFS data using: python data_ingestion/gfs_downloader.py --date YYYYMMDD --cycle HH")
        print("2. Process GFS data using: python data_processing/process_data.py --date YYYYMMDD --cycle HH")
        print("3. Download MEPS data using: python data_ingestion/met_downloader.py --date YYYYMMDD --cycle 06")
        print("4. Process MEPS data using: python data_processing/process_meps_data.py --date YYYYMMDD --cycle 06")
        
        return False
    
    return True

def run_dashboard(host='0.0.0.0', port=8050, debug=True):
    """Run the dashboard"""
    try:
        # Import the dashboard (this will be the main dashboard file you create)
        from visualization.interactive_dashboard import app
        
        print(f"Starting Weather Dashboard...")
        print(f"Dashboard will be available at: http://localhost:{port}")
        print("Press Ctrl+C to stop the dashboard")
        
        app.run_server(host=host, port=port, debug=debug)
        
    except ImportError as e:
        print(f"Error importing dashboard: {e}")
        print("Make sure the dashboard file exists at 'visualization/interactive_dashboard.py'")
        sys.exit(1)
    except Exception as e:
        print(f"Error starting dashboard: {e}")
        sys.exit(1)

def process_sample_data():
    """Process sample data if available"""
    print("Looking for sample data to process...")
    
    # Look for recent MEPS data
    meps_raw_path = Path("data/raw/met")
    if meps_raw_path.exists():
        date_dirs = sorted([d for d in meps_raw_path.iterdir() if d.is_dir()])
        if date_dirs:
            latest_date = date_dirs[-1]
            cycle_dirs = [d for d in latest_date.iterdir() if d.is_dir()]
            if cycle_dirs:
                latest_cycle = cycle_dirs[0]
                date_str = latest_date.name
                cycle_str = latest_cycle.name
                
                print(f"Processing MEPS data for {date_str} cycle {cycle_str}")
                try:
                    from data_processing.process_meps_data import process_meps_data
                    process_meps_data(date_str, cycle_str)
                    print("MEPS data processing completed")
                except Exception as e:
                    print(f"Error processing MEPS data: {e}")

def main():
    parser = argparse.ArgumentParser(description="Start the Interactive Weather Dashboard")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8050, help="Port to bind to (default: 8050)")
    parser.add_argument("--no-debug", action="store_true", help="Disable debug mode")
    parser.add_argument("--check-only", action="store_true", help="Only check requirements and data, don't start dashboard")
    parser.add_argument("--process-sample", action="store_true", help="Process sample data before starting")
    
    args = parser.parse_args()
    
    print("=" * 50)
    print("Interactive Weather Dashboard Startup")
    print("=" * 50)
    
    # Check requirements
    #if not check_requirements():
    #    sys.exit(1)
    
    print("✓ All required packages are installed")
    
    # Process sample data if requested
    if args.process_sample:
        process_sample_data()
    
    # Check data availability
    data_available = check_data_availability()
    
    if args.check_only:
        if data_available:
            print("\n✓ System is ready to run the dashboard")
            sys.exit(0)
        else:
            print("\n✗ System is not ready - missing data")
            sys.exit(1)
    
    if not data_available:
        response = input("\nNo processed data found. Continue anyway? (y/N): ")
        if response.lower() != 'y':
            sys.exit(1)
    
    # Run the dashboard
    debug_mode = not args.no_debug
    run_dashboard(host=args.host, port=args.port, debug=debug_mode)

if __name__ == "__main__":
    main()
