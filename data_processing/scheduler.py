"""
Scheduler Module
Handles automated data extraction using cron-like scheduling and file monitoring
"""

import schedule
import time
import threading
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from loguru import logger
import sys
import os
sys.path.append('src')
sys.path.append('config')
from data_extractor import GFSDataExtractor
from config import LOG_FILE, LOG_LEVEL

class DataFileHandler(FileSystemEventHandler):
    """File system event handler for monitoring data directory"""
    
    def __init__(self, extractor):
        self.extractor = extractor
        
    def on_created(self, event):
        if not event.is_directory:
            logger.info(f"New file detected: {event.src_path}")
            # Trigger data processing if needed
            
    def on_modified(self, event):
        if not event.is_directory:
            logger.info(f"File modified: {event.src_path}")

class GFSScheduler:
    def __init__(self):
        self.setup_logging()
        self.extractor = GFSDataExtractor()
        self.observer = None
        
    def setup_logging(self):
        """Setup logging with loguru"""
        logger.add(LOG_FILE, rotation="10 MB", level=LOG_LEVEL)
        logger.info("GFS Scheduler initialized")
        
    def run_scheduled_extraction(self):
        """Run the scheduled data extraction"""
        try:
            logger.info("Starting scheduled GFS data extraction")
            current_time = datetime.now()
            date_str = current_time.strftime('%Y%m%d')
            
            # Determine which cycle to run based on current time
            hour = current_time.hour
            if hour < 6:
                cycle = "00"
            elif hour < 12:
                cycle = "06"
            elif hour < 18:
                cycle = "12"
            else:
                cycle = "18"
                
            self.extractor.run_extraction(date_str, cycle)
            logger.info("Scheduled extraction completed successfully")
            
            # After extraction, run the R script for analysis
            self.run_analysis_script(date_str, cycle)
            
        except Exception as e:
            logger.error(f"Scheduled extraction failed: {e}")
            
    def run_analysis_script(self, date_str, cycle):
        """Run the analysis script"""
        try:
            logger.info(f"Running analysis script for {date_str} cycle {cycle}")
            import subprocess
            
            # Path to the script
            py_script_path = "src/analysis.py"
            
            # Command to execute the script with arguments
            #command = ["python ", py_script_path, date_str, cycle]
            command = ["python", py_script_path, "--date", date_str, "--cycle", cycle]
            
            # Execute the command
            result = subprocess.run(command, capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.info("Analysis script executed successfully")
                logger.info(f"Analysis script output:\n{result.stdout}")
            else:
                logger.error(f"Analysis script execution failed with return code {result.returncode}")
                logger.error(f"Analysis script error output:\n{result.stderr}")
                
        except Exception as e:
            logger.error(f"Failed to run analysis script: {e}")
            
    def setup_file_monitoring(self):
        """Setup file system monitoring using watchdog"""
        try:
            event_handler = DataFileHandler(self.extractor)
            self.observer = Observer()
            
            # Monitor data directories
            self.observer.schedule(event_handler, "data/raw", recursive=True)
            self.observer.schedule(event_handler, "data/processed", recursive=True)
            
            self.observer.start()
            logger.info("File monitoring started")
            
        except Exception as e:
            logger.error(f"Failed to setup file monitoring: {e}")
            
    def setup_schedule(self):
        """Setup cron-like scheduling"""
        # Schedule data extraction 4 times a day (after each GFS run)
        schedule.every().day.at("01:00").do(self.run_scheduled_extraction)  # After 00Z run
        schedule.every().day.at("07:00").do(self.run_scheduled_extraction)  # After 06Z run
        schedule.every().day.at("13:00").do(self.run_scheduled_extraction)  # After 12Z run
        schedule.every().day.at("19:00").do(self.run_scheduled_extraction)  # After 18Z run
        
        logger.info("Scheduled jobs configured")
        
    def run_scheduler(self):
        """Main scheduler loop"""
        logger.info("Starting GFS data pipeline scheduler")
        
        # Setup file monitoring
        self.setup_file_monitoring()
        
        # Setup scheduled jobs
        self.setup_schedule()
        
        # Run initial extraction
        logger.info("Running initial data extraction")
        self.run_scheduled_extraction()
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
                
        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user")
            if self.observer:
                self.observer.stop()
                self.observer.join()
                
    def run_manual_extraction(self, date_str=None, cycle=None):
        """Run manual data extraction"""
        logger.info("Running manual data extraction")
        self.extractor.run_extraction(date_str, cycle)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='GFS Data Pipeline Scheduler')
    parser.add_argument('--mode', choices=['scheduler', 'manual'], default='scheduler',
                       help='Run mode: scheduler (continuous) or manual (one-time)')
    parser.add_argument('--date', help='Date for manual extraction (YYYYMMDD)')
    parser.add_argument('--cycle', choices=['00', '06', '12', '18'], 
                       help='GFS cycle for manual extraction')
    
    args = parser.parse_args()
    scheduler = GFSScheduler()
    
    if args.mode == 'manual':
        scheduler.run_manual_extraction(args.date, args.cycle)
    else:
        scheduler.run_scheduler()
