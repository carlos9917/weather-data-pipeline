"""
Weather Data Pipeline Scheduler

Orchestrates downloading, processing, and visualization for multiple data sources:
- GFS (Global Forecast System)
- MET Nordic (MET Norway 1.0km)
- MEPS 2.5km (MetCoOp Ensemble Prediction System)

Supports selective execution of individual data sources or combinations.
"""

import subprocess
import argparse
import sys
import os

# Define pipeline steps grouped by data source
PIPELINE_STEPS = {
    'gfs': [
        ("data_ingestion/gfs_downloader.py", "Downloading GFS data"),
        ("data_processing/process_data.py", "Processing GFS data"),
        ("visualization/create_visualizations.py", "Creating GFS visualizations"),
    ],
    'met': [
        ("data_ingestion/met_downloader.py", "Downloading MET Nordic data"),
        ("data_processing/process_met_data.py", "Processing MET data"),
        ("visualization/create_met_visualizations.py", "Creating MET visualizations"),
    ],
    'meps25': [
        ("data_ingestion/meps25_downloader.py", "Downloading MEPS 2.5km data"),
        ("data_processing/process_meps25_data.py", "Processing MEPS 2.5km data"),
        ("visualization/create_meps25_visualizations.py", "Creating MEPS 2.5km visualizations"),
    ],
}

# Default source order when running all
DEFAULT_SOURCE_ORDER = ['gfs', 'met', 'meps25']


def run_pipeline(date, cycle, sources=None, skip_download=False, skip_process=False, skip_visualize=False):
    """
    Runs the weather data pipeline for selected data sources.

    Args:
        date (str): The date in YYYYMMDD format.
        cycle (str): The cycle ('00', '06', '12', '18').
        sources (list): List of data sources to run. If None, runs all.
        skip_download (bool): Skip download steps.
        skip_process (bool): Skip processing steps.
        skip_visualize (bool): Skip visualization steps.
    """
    if sources is None:
        sources = DEFAULT_SOURCE_ORDER
    
    # Validate sources
    invalid_sources = [s for s in sources if s not in PIPELINE_STEPS]
    if invalid_sources:
        print(f"Error: Unknown data source(s): {invalid_sources}")
        print(f"Valid sources: {list(PIPELINE_STEPS.keys())}")
        sys.exit(1)
    
    print("=" * 60)
    print(f"Weather Data Pipeline")
    print(f"Date: {date}, Cycle: {cycle}Z")
    print(f"Sources: {', '.join(sources)}")
    print("=" * 60)
    
    # Collect scripts to run
    scripts_to_run = []
    for source in sources:
        for script_path, description in PIPELINE_STEPS[source]:
            # Filter based on skip flags
            if skip_download and 'downloader' in script_path:
                continue
            if skip_process and 'process' in script_path:
                continue
            if skip_visualize and 'visualization' in script_path:
                continue
            scripts_to_run.append((script_path, description))
    
    if not scripts_to_run:
        print("No steps to run (all skipped).")
        return
    
    print(f"\nSteps to execute: {len(scripts_to_run)}")
    for i, (script_path, desc) in enumerate(scripts_to_run, 1):
        print(f"  {i}. {desc}")
    print()
    
    # Execute scripts
    for i, (script_path, description) in enumerate(scripts_to_run, 1):
        print(f"\n[{i}/{len(scripts_to_run)}] {description} for {date} cycle {cycle}")
        print("-" * 50)

        script_abs_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            script_path
        )

        result = subprocess.run(
            [sys.executable, script_abs_path, "--date", date, "--cycle", cycle]
        )

        if result.returncode != 0:
            print(f"\nError: {script_path} failed with exit code {result.returncode}")
            sys.exit(1)
        else:
            print(f"✓ Completed: {description}")

    print("\n" + "=" * 60)
    print("Pipeline completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run the weather data pipeline with selective source execution.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full pipeline (all sources)
  python pipeline_scheduler.py --date 20231115 --cycle 06
  
  # Run only MEPS 2.5km
  python pipeline_scheduler.py --date 20231115 --cycle 06 --sources meps25
  
  # Run GFS and MEPS 2.5km (skip MET Nordic)
  python pipeline_scheduler.py --date 20231115 --cycle 06 --sources gfs meps25
  
  # Run only MET Nordic
  python pipeline_scheduler.py --date 20231115 --cycle 06 --sources met
  
  # Skip download step (use existing raw data)
  python pipeline_scheduler.py --date 20231115 --cycle 06 --sources meps25 --skip-download
  
  # Only create visualizations (skip download and processing)
  python pipeline_scheduler.py --date 20231115 --cycle 06 --skip-download --skip-process

Available sources:
  gfs     - GFS global model (0.25 degree)
  met     - MET Nordic regional model (1.0km)
  meps25  - MEPS deterministic model (2.5km)
"""
    )
    parser.add_argument("--date", required=True, 
                        help="Date in YYYYMMDD format.")
    parser.add_argument("--cycle", required=True, 
                        help="Cycle (00, 06, 12, 18).")
    parser.add_argument("--sources", nargs='+', default=None,
                        choices=['gfs', 'met', 'meps25'],
                        help="Data sources to run. Default: all (gfs, met, meps25)")
    parser.add_argument("--skip-download", action="store_true",
                        help="Skip download steps (use existing raw data)")
    parser.add_argument("--skip-process", action="store_true",
                        help="Skip processing steps (use existing Zarr data)")
    parser.add_argument("--skip-visualize", action="store_true",
                        help="Skip visualization steps")
    
    args = parser.parse_args()

    run_pipeline(
        args.date, 
        args.cycle, 
        sources=args.sources,
        skip_download=args.skip_download,
        skip_process=args.skip_process,
        skip_visualize=args.skip_visualize
    )
