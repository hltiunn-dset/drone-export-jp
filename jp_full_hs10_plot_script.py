#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
JP Export Visualization Pipeline (HS10 = 8806)
Exact structural replica of TW full_hs10_plot_script_v2.py,
rewritten for Japan.

Outputs:
    JP_cleaned_export_by_hs10.csv
    All JP_* prefixed PNGs from jp_plot_01_to_02 and jp_plot_03_to_06.
"""

import pandas as pd
import numpy as np
import re
from pathlib import Path
import subprocess
import datetime as _dt
import calendar as _cal

# ================================================================
# 0. JP Export Directory (as requested by user)
# ================================================================
EXPORT_DIR = Path(
    "~/Library/Mobile Documents/com~apple~CloudDocs/github/drone-export-jp"
).expanduser()
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

JP_RAW = EXPORT_DIR / "jp_8806_by_country_month_2023_2025.csv"

ENCODING = "utf-8-sig"

# ================================================================
# 1. JP HS10 → Group, Class, MTOW Mapping
# Mirrors the TW structure (tuple of 3 fields)
# ================================================================

# JP-specific mapping (user may update depending on Japan's category logic)
hs10_group_class_map_jp = {
    "8806.10.00.00": ("Group 5", "Class III", "Passenger UAV"),
    "8806.21.00.00": ("Group 1", "Class I", "≤250g"),
    "8806.22.00.00": ("Group 1", "Class I", "250g–7kg"),
    "8806.23.00.00": ("Group 2", "Class I", "7kg–25kg"),
    "8806.24.00.00": ("Group 3", "Class II", "25kg–150kg"),
    "8806.29.00.00": ("Group 4/5", "Class III", ">150kg"),
    "8806.91.00.00": ("Group 1", "Class I", "≤250g"),
    "8806.92.00.00": ("Group 1", "Class I", "250g–7kg"),
    "8806.93.00.00": ("Group 2", "Class I", "7kg–25kg"),
    "8806.94.00.00": ("Group 3", "Class II", "25kg–150kg"),
    "8806.99.00.00": ("Group 4/5", "Class III", ">150kg"),
}

def map_hs10_to_group_tuple(hs):
    return hs10_group_class_map_jp.get(hs, (None, None, None))

# ================================================================
# 2. Japanese → English Country Translation (mirrors TW structure)
# ================================================================
jp_country_map = {
    "大韓民国": "South Korea",
    "ベトナム": "Vietnam",
    "マレーシア": "Malaysia",
    "インド": "India",
    "インドネシア": "Indonesia",
    "タイ": "Thailand",
    "米国": "United States",
    "アメリカ合衆国": "United States",
    "イギリス": "United Kingdom",
    "英国": "United Kingdom",
    "フランス": "France",
    "ドイツ": "Germany",
    "台湾": "Taiwan",
    "フィリピン": "Philippines",
    "シンガポール": "Singapore",
    "オランダ": "The Netherlands",
    "スペイン": "Spain",
    "エジプト": "Egypt",
    "オーストラリア": "Australia",
    "モンゴル": "Mongolia",
    "香港": "Hong Kong",
    "ウクライナ": "Ukraine",
    "ブラジル": "Brazil",
    "スイス": "Switzerland",
    "イタリア": "Italy",
    "カナダ": "Canada",
    "コロンビア": "Colombia",
    "チリ": "Chile",
    "アルゼンチン": "Argentina",
    "南アフリカ共和国": "South Africa",
    "ザンビア": "Zambia",
    "サウジアラビア": "Saudi Arabia"
}

def translate_country_jp(x):
    return jp_country_map.get(x, x)

# ================================================================
# 3. Convert "yyyymm" into quarters like TW qtr function
# ================================================================
def yyyymm_to_qtr(yyyymm):
    year = int(str(yyyymm)[:4])
    month = int(str(yyyymm)[4:6])
    q = (month - 1) // 3 + 1
    return f"{year} Q{q}"

# ================================================================
# 4. Load JP dataset
# ================================================================
df = pd.read_csv(JP_RAW, encoding=ENCODING)

# Extract Japanese country name (format: "103_大韓民国")
df["country_jp"] = df["area_name"].apply(lambda x: x.split("_", 1)[1])
df["country"] = df["country_jp"].apply(translate_country_jp)

# Create qtr and period
df["yyyymm"] = df["yyyymm"].astype(str)
df["qtr"] = df["yyyymm"].apply(yyyymm_to_qtr)

# Period (H1/H2) for 2023–2025 (mirror TW structure)
def qtr_to_period(q):
    if "2023" in q:
        return "2023 H1" if q in ("2023 Q1", "2023 Q2") else "2023 H2"
    if "2024" in q:
        return "2024 H1" if q in ("2024 Q1", "2024 Q2") else "2024 H2"
    if "2025" in q:
        return "2025 H1" if q in ("2025 Q1", "2025 Q2") else "2025 H2"
    return None

df["period"] = df["qtr"].apply(qtr_to_period)

# ================================================================
# 5. Create classification columns: US_Group, NATO_Class, MTOW
# ================================================================
df[["US_Group", "NATO_Class", "MTOW"]] = df["hs10"].apply(
    lambda hs: pd.Series(map_hs10_to_group_tuple(hs))
)

# Re-export flag (Japan dataset has no explicit flag → set False)
df["is_reexport"] = False

# ================================================================
# 6. Quantity & Value (mirror TW "Quanity" and "K JPY")
# ================================================================
df["Quanity"] = pd.to_numeric(df["NO"], errors="coerce")
df["K JPY"] = pd.to_numeric(df["Yen_thousand"], errors="coerce")

# ================================================================
# 7. Final output columns (match TW EXACTLY)
# ================================================================
final_cols = [
    "qtr", "period", "country", "hs10",
    "US_Group", "NATO_Class", "MTOW",
    "Quanity", "K JPY",
    "is_reexport"
]

df_final = df[final_cols]

# ================================================================
# 8. Save as JP_cleaned_export_by_hs10.csv
# ================================================================
OUT_CLEAN = EXPORT_DIR / "JP_cleaned_export_by_hs10.csv"
df_final.to_csv(OUT_CLEAN, index=False, encoding="utf-8-sig")
print(f"✅ JP_cleaned_export_by_hs10.csv created at {OUT_CLEAN}")

# ================================================================
# 9. Run JP plot scripts (mirroring TW subprocess calls)
# ================================================================
from jp_plot_01_to_02 import run_plot_01_02
from jp_plot_03_to_06 import run_plot_03_04, run_plot_05_06

# === HS10 category ===
run_plot_01_02("hs10")
run_plot_03_04("hs10")
run_plot_05_06("hs10")
print("✅ HS10 plots done")

# === US_Group category ===
run_plot_01_02("US_Group")
run_plot_03_04("US_Group")
run_plot_05_06("US_Group")
print("✅ US_Group plots done")

# === NATO_Class category ===
run_plot_01_02("NATO_Class")
run_plot_03_04("NATO_Class")
run_plot_05_06("NATO_Class")
print("✅ NATO_Class plots done")

print("🎉 ALL JP PLOTS COMPLETED")
