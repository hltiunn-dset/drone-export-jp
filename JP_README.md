# Japan 8806 Export Visualization Pipeline (JP)

A full replication of the Taiwan (TW) export workflow, adapted for Japan’s export statistics API.

## Overview
This repository implements a Japan-specific export data processing and visualization workflow for HS10 export codes under 8806 (unmanned aircraft & parts). It mirrors the structure, logic, file outputs, and visualization style of the Taiwan (TW) export pipeline while keeping both systems separated.

## Updated Master Script
A new unified pipeline controller has been added:

**`jp_pull_and_run_pipeline.py`**

This script:
1. Pulls raw data from the Japan e-Stat API  
2. Saves the raw CSV into the JP folder  
3. Validates the dataset  
4. Automatically triggers the full JP visualization pipeline  
5. Produces all PNG plots and processed CSV files  

This fully automates the entire JP workflow end‑to‑end.

## Directory Structure
~/Library/Mobile Documents/com~apple~CloudDocs/github/drone-export-jp/

Contents include:
- jp_pull_and_run_pipeline.py           # NEW master script
- jp_8806_by_country_month_2023_2025.csv
- JP_cleaned_export_by_hs10.csv
- jp_full_hs10_plot_script_v2.py
- jp_plot_01_to_02.py
- jp_plot_03_to_06.py
- PNG outputs (JP_01_*, JP_02_*, JP_03_*, JP_04_*, JP_05_*, JP_06_*)
- JP_README.md

## Running the Entire JP Pipeline
To fetch data and generate all plots:

```
python jp_pull_and_run_pipeline.py
```

This will:
- connect to the Japan government API  
- fetch HS10=8806 export data  
- write jp_8806_by_country_month_2023_2025.csv  
- run jp_full_hs10_plot_script_v2.py  
- create all JP_* PNG visualizations  

## Files Produced
### Data Files
- **JP_cleaned_export_by_hs10.csv** – standardized and fully categorized dataset  
- **jp_8806_by_country_month_2023_2025.csv** – raw API export  

### Plots
- JP_01_Counts_total_*.png  
- JP_02_Percent_total_*.png  
- JP_03_Value_*.png  
- JP_04_ValuePct_*.png  
- JP_05_Value_Total_*.png  
- JP_06_ValuePct_Total_*.png  

All plots mirror the Taiwan workflow exactly, with consistent style and naming.

## Notes
- All JP files are safely isolated from Taiwan’s `drone-export/` folder.
- File naming and folder structure match the TW pipeline for comparability.
- The JP pipeline uses `NO` as quantity and `Yen_thousand` as export value, in full alignment with the TW logic.
