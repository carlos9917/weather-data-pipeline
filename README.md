# Weather Data Pipeline

This project is a prototype system to stream and visualize operational weather forecasts. It demonstrates the ability to source data, design a pipeline, select relevant parameters, and produce actionable visualizations.

## Directory Structure

```
weather-data-pipeline/
├── data_ingestion/
│   ├── __init__.py
│   ├── gfs_downloader.py
│   └── regional_model_downloader.py
├── data_processing/
│   ├── __init__.py
│   └── process_data.py
├── storage/
│   └── processed_data/
├── visualization/
│   ├── __init__.py
│   └── create_visualizations.py
├── orchestration/
│   ├── __init__.py
│   └── pipeline_scheduler.py
├── config.py
├── main.py
├── requirements.txt
└── README.md
```

## Component Breakdown

### 1. `data_ingestion/`

*   **Purpose:** Fetches raw forecast data from the global and regional weather models.
*   **`gfs_downloader.py`:** This script will download data from the Global Forecast System (GFS).
*   **`regional_model_downloader.py`:** This script will download data from a regional model covering Northern Europe/Scandinavia.

### 2. `data_processing/`

*   **Purpose:** Transforms the raw data into a clean and usable format.
*   **`process_data.py`:** This script will read the downloaded GRIB or NetCDF files, select the required variables (precipitation, cloud cover, wind), and convert the data to a more efficient format like Zarr or NetCDF.

### 3. `storage/`

*   **Purpose:** Stores the processed data.
*   **`processed_data/`:** This directory will hold the processed data in a format like Zarr or NetCDF.

### 4. `visualization/`

*   **Purpose:** Creates visualizations from the processed data.
*   **`create_visualizations.py`:** This script will generate time-stepped map visualizations of the selected parameters.

### 5. `orchestration/`

*   **Purpose:** Automates the pipeline to run continuously.
*   **`pipeline_scheduler.py`:** This script will schedule the pipeline to run at regular intervals.

### 6. Root Files

*   **`config.py`:** Stores configuration variables for the pipeline.
*   **`main.py`:** The main entry point for the application.
*   **`requirements.txt`:** Lists the Python dependencies for the project.
*   **`README.md`:** This file, providing an overview of the project.