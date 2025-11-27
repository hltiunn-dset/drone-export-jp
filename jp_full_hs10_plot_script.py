#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
JP Full Trade Visualization Pipeline (EXPORT + IMPORT)
------------------------------------------------------

Updated to match jp_pull_and_run_trade_pipeline.py:
- Reads combined import + export dataset
- Splits into EXPORT and IMPORT
- Applies HS10 → (US_Group, NATO_Class, MTOW) mapping
- Translates JP country names → English
- Creates qtr and period variables (TW-style)
- Saves:
      JP_cleaned_export_by_hs10.csv
      JP_cleaned_import_by_hs10.csv
- Runs all plots using jp_plot_01_to_02 and jp_plot_03_to_06

This is the synchronized counterpart of TW full_hs10_plot_script_v2.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import subprocess

# ================================================================
# 0. Directories (same as pipeline)
# ================================================================
BASE_DIR = Path(
    "~/Library/Mobile Documents/com~apple~CloudDocs/github/drone-export-jp"
).expanduser()

INPUT_FILE = BASE_DIR / "jp_trade_export_import_8806_monthly.csv"

OUT_EXPORT = BASE_DIR / "JP_cleaned_export_by_hs10.csv"
OUT_IMPORT = BASE_DIR / "JP_cleaned_import_by_hs10.csv"

ENC = "utf-8-sig"

# ================================================================
# 1. HS10 → (US_Group, NATO_Class, MTOW)
#    Exactly the same as your updated jp_pull_and_run_trade_pipeline
# ================================================================
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

def map_hs10_to_tuple(hs):
    return hs10_group_class_map_jp.get(hs, (None, None, None))

# ================================================================
# 2. JP → English Country Name mapping
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
    "サウジアラビア": "Saudi Arabia",
    "東ティモール": "Timor-Leste",
    "イスラエル": "Israel",
    "ノルウェー": "Norway",
    "ベルギー": "Belgium",
    "ハンガリー": "Hungary",
    "ラトビア": "Latvia",
    "スロベニア": "Slovenia",
    "コモロ": "Comoros",
    "デンマーク": "Denmark",
    "ギリシャ": "Greece",
    "トルコ": "Türkiye",
    "チェコ": "Czechia",
    "メキシコ": "Mexico",
    "スウェーデン": "Sweden",
    "リトアニア": "Lithuania",
    "エクアドル": "Ecuador",
    "中華人民共和国": "PRC"
}

def translate_jp_country(x):
    return jp_country_map.get(x, x)

# ================================================================
# 3. Quarter + Period conversion
# ================================================================
def yyyymm_to_qtr(yyyymm):
    y = int(str(yyyymm)[:4])
    m = int(str(yyyymm)[4:6])
    q = (m - 1)//3 + 1
    return f"{y} Q{q}"

def qtr_to_period(q):
    y = q.split()[0]
    if q.endswith("Q1") or q.endswith("Q2"):
        return f"{y} H1"
    return f"{y} H2"

# ================================================================
# 4. Load combined dataset
# ================================================================
print(f"📂 Loading combined JP trade data from: {INPUT_FILE}")
df = pd.read_csv(INPUT_FILE, dtype=str)

# Convert types
df["yyyymm"] = df["yyyymm"].astype(str)
df["NO"] = pd.to_numeric(df["NO"], errors="coerce")
df["Yen_thousand"] = pd.to_numeric(df["Yen_thousand"], errors="coerce")

# ================================================================
# 5. Extract country (JP → English)
# ================================================================
df["country_jp"] = df["area_name"].apply(lambda x: x.split("_",1)[1])
df["country"] = df["country_jp"].apply(translate_jp_country)

# ================================================================
# 6. Period variables
# ================================================================
df["qtr"] = df["yyyymm"].apply(yyyymm_to_qtr)
df["period"] = df["qtr"].apply(qtr_to_period)

# ================================================================
# 7. HS10 classifications
# ================================================================
df[["US_Group", "NATO_Class", "MTOW"]] = df["hs10"].apply(
    lambda x: pd.Series(map_hs10_to_tuple(x))
)

# Japan has no re-export flag
df["is_reexport"] = False

# Mirror TW naming style
df["Quanity"] = df["NO"]
df["K JPY"] = df["Yen_thousand"]

# ================================================================
# 8. Select final output columns (mirror TW exactly)
# ================================================================
final_cols = [
    "qtr", "period", "country", "hs10",
    "US_Group", "NATO_Class", "MTOW",
    "Quanity", "K JPY",
    "is_reexport"
]

# ================================================================
# 9. Split EXPORT + IMPORT
# ================================================================
df_export = df[df["flow"] == "EXPORT"][final_cols].copy()
df_import = df[df["flow"] == "IMPORT"][final_cols].copy()

print(f"EXPORT rows: {len(df_export)}")
print(f"IMPORT rows: {len(df_import)}")

# ================================================================
# 10. Save cleaned files
# ================================================================
df_export.to_csv(OUT_EXPORT, index=False, encoding=ENC)
df_import.to_csv(OUT_IMPORT, index=False, encoding=ENC)

print(f"💾 Saved: {OUT_EXPORT}")
print(f"💾 Saved: {OUT_IMPORT}")

# ================================================================
# 11. Run JP plot scripts (TW-style subprocess)
# ================================================================
def run_plot(script, flow):
    cmd = ["python", str(BASE_DIR / script), flow]
    print("▶ Running:", " ".join(cmd))
    r = subprocess.run(cmd, capture_output=True, text=True)
    print(r.stdout)
    print(r.stderr)

print("\n===== GENERATING EXPORT PLOTS =====")
run_plot("jp_plot_01_to_02.py", "EXPORT")
run_plot("jp_plot_03_to_06.py", "EXPORT")

print("\n===== GENERATING IMPORT PLOTS =====")
run_plot("jp_plot_01_to_02.py", "IMPORT")
run_plot("jp_plot_03_to_06.py", "IMPORT")

print("\n🎉 ALL JP PLOTS COMPLETED SUCCESSFULLY")
