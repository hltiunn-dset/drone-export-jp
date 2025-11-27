# Japan 8806 Trade Visualization Pipeline (JP)
A full replication of the Taiwan (TW) export workflow, now extended to include **both EXPORT and IMPORT** flows from Japan’s e‑Stat API (HS10 under 8806).

This version updates the README to match the **combined EXPORT+IMPORT pipeline**, the new unified master script, and the fully automated visualization workflow.

---

## 🚀 What This Pipeline Does
The Japan trade pipeline now performs:

### **1. Automatic Data Retrieval (EXPORT + IMPORT)**
Using `jp_pull_and_run_pipeline.py`, the system:
- Fetches **EXPORT** data from statsDataId = `0003425293`
- Fetches **IMPORT** data from statsDataId = `0003425294`
- Validates metadata structure for both flows
- Retrieves all HS10 codes under 8806
- Pulls NO / KG / Yen values across all countries and months
- Standardizes HS10 into `8806.xx.xx.xx`

### **2. Combined Trade Dataset**
After retrieval, the script:
- Builds **EXPORT** and **IMPORT** datasets separately  
- Adds a `flow` column (`EXPORT` or `IMPORT`)
- Merges into a single unified file:

```
JP_trade_8806_export_import_2023_2025.csv
```

### **3. Cleaned Export-Only Dataset (TW-Compatible)**
Because the TW plotting scripts visualize EXPORT only, the pipeline also produces:

```
JP_cleaned_export_by_hs10.csv
```

This file feeds directly into all 6 JP visualization modules.

---

## 📂 Directory Structure  
Located at:

```
~/Library/Mobile Documents/com~apple~CloudDocs/github/drone-export-jp/
```

Includes:

```
jp_pull_and_run_pipeline.py       # MASTER script (EXPORT + IMPORT)
jp_full_hs10_plot_script.py       # JP cleaned → PNG pipeline
jp_plot_01_to_02.py               # Plot modules
jp_plot_03_to_06.py
jp_8806_by_country_month_2023_2025.csv     # raw export dataset
JP_trade_8806_export_import_2023_2025.csv   # combined flows
JP_cleaned_export_by_hs10.csv               # cleaned dataset for plots
plot/                                       # JP_* png outputs
README.md                                   # THIS FILE
```

---

## ▶️ Running the Entire Japan Pipeline

Run:

```
python jp_pull_and_run_pipeline.py
```

This will:
1. Fetch EXPORT data  
2. Fetch IMPORT data  
3. Save raw datasets  
4. Merge flows  
5. Clean EXPORT dataset  
6. Auto‑trigger all plot scripts  
7. Save all JP_* prefixed PNGs  

Completely hands‑free.

---

## 🗂 Files Generated

### **Data Files**
| File | Description |
|------|-------------|
| `jp_8806_by_country_month_2023_2025.csv` | Raw EXPORT API pull |
| `JP_trade_8806_export_import_2023_2025.csv` | Combined EXPORT + IMPORT |
| `JP_cleaned_export_by_hs10.csv` | Fully categorized and TW‑compatible EXPORT dataset |

---

## 📊 Plot Outputs
All PNGs appear under:

```
/plot/
```

Generated exactly like TW, but JP‑prefixed:

### **01–02: Units (NO)**
```
JP_01_Counts_total_*.png
JP_02_Percent_total_*.png
JP_01_Counts_{category}_*.png
JP_02_Percent_{category}_*.png
```

### **03–04: Value (K JPY)**
```
JP_03_Value_*.png
JP_04_ValuePct_*.png
```

### **05–06: H1/H2 Period Plots**
```
JP_05_Value_Total_*.png
JP_06_ValuePct_Total_*.png
```

---

## 🧩 HS10 Mapping & Classification
The JP pipeline maintains full compatibility with TW classification:

- `US_Group`
- `NATO_Class`
- `MTOW`
- `country`
- `qtr`, `period`
- `is_reexport` (always False for JP)

---

## ✔️ Notes & Behavior Guarantees
- JP pipeline mirrors TW 100% in structure and logic  
- JP files **never** overwrite TW files  
- All naming conventions follow DSET’s drone export visualization standard  
- HS10 formatting strictly enforced as:  
  ```
  8806.xx.xx.xx
  ```

---

## 📎 Version
**Updated: Automatically synchronized with your latest JP scripts and combined-flow design.**
