#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
JP Trade Downloader (EXPORT + IMPORT) + Auto-Plot Pipeline Runner
-----------------------------------------------------------------
1. Downloads Japan HS10=8806 EXPORT **and** IMPORT data from e-Stat API
2. Combines both flows into one dataset with column: flow ∈ {EXPORT, IMPORT}
3. Validates dataset structure
4. Saves unified CSV into drone-export-jp folder
5. Automatically triggers: jp_full_hs10_plot_script.py
"""

import os
import requests
import pandas as pd
import subprocess
from pathlib import Path
import sys

# ================================================================
# 0. CONFIG
# ================================================================
APP_ID = "c78f352b95db85e0598f2bd54d8c6d12b7560686"

# EXPORT + IMPORT tables (Option 3)
TRADE_TYPES = {
    "EXPORT": "0003425293",
    "IMPORT": "0003425294",
}

BASE_URL_META = "https://api.e-stat.go.jp/rest/3.0/app/json/getMetaInfo"
BASE_URL_DATA = "https://api.e-stat.go.jp/rest/3.0/app/json/getStatsData"

YEARS = ["2022", "2023", "2024", "2025"]

EXPORT_DIR = Path(
    "~/Library/Mobile Documents/com~apple~CloudDocs/github/drone-export-jp"
).expanduser()
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = EXPORT_DIR / "jp_trade_export_import_8806_monthly.csv"
ENC = "utf-8-sig"

# ================================================================
# HS10 Formatting
# ================================================================
HS10_LIST = [
    "880610000",
    "880621000",
    "880622000",
    "880623000",
    "880624000",
    "880629000",
    "880630000",
    "880640000",
    "880650000",
    "880660000",
    "880690000",
    "880691000",
    "880692000",
    "880693000",
    "880694000",
    "880699000",
]

def format_hs10(hs):
    """
    Convert Japanese HS9 (e.g. '880692000') → HS10 formatted '8806.92.00.00'
    """
    raw = str(hs).strip()
    if len(raw) != 9:
        raise ValueError(f"Unexpected HS code length: {raw}")
    return f"{raw[0:4]}.{raw[4:6]}.{raw[6:8]}.00"

# ================================================================
# 1. FETCH METADATA (per flow)
# ================================================================
def get_meta(stats_id):
    params = {"appId": APP_ID, "statsDataId": stats_id, "lang": "J"}
    r = requests.get(BASE_URL_META, params=params)
    r.raise_for_status()
    return r.json()

def extract_meta_maps(meta):
    class_objs = meta["GET_META_INFO"]["METADATA_INF"]["CLASS_INF"]["CLASS_OBJ"]

    def get_class_obj(class_id):
        for obj in class_objs:
            if obj["@id"] == class_id:
                return obj
        raise ValueError(f"Class {class_id} not found")

    area_obj = get_class_obj("area")
    cat02_obj = get_class_obj("cat02")

    area_map = {c["@code"]: c["@name"] for c in area_obj["CLASS"]}

    # Parse cat02 → (month, var)
    cat02_month_map = {}
    for c in cat02_obj["CLASS"]:
        code = c["@code"]
        name = c["@name"]
        if "月" in name and "_" in name:
            month_str, kind = name.split("_")
            month = int(month_str.replace("月", ""))
            if kind == "数量1":
                var = "NO"
            elif kind == "数量2":
                var = "KG"
            elif kind == "金額":
                var = "YEN"
            else:
                continue
            cat02_month_map[code] = {"month": month, "var": var}

    cat02_codes = sorted(cat02_month_map.keys(), key=int)
    return area_map, cat02_month_map, cat02_codes

# ================================================================
# 2. FETCH YEAR (per flow)
# ================================================================
def fetch_year(year, stats_id, area_map, cat02_map, cat02_codes):
    time_code = f"{year}000000"
    params = {
        "appId": APP_ID,
        "statsDataId": stats_id,
        "lang": "J",
        "cdTime": time_code,
        "cdCat01": ",".join(HS10_LIST),
        "cdCat02": ",".join(cat02_codes),
        "limit": 100000,
    }

    r = requests.get(BASE_URL_DATA, params=params)
    r.raise_for_status()
    js = r.json()

    try:
        values = js["GET_STATS_DATA"]["STATISTICAL_DATA"]["DATA_INF"]["VALUE"]
    except KeyError:
        raise RuntimeError(f"No VALUE found for {year} in stats_id {stats_id}")

    if isinstance(values, dict):
        values = [values]

    rows = []
    for v in values:
        hs10 = v["@cat01"]
        area_code = v["@area"]
        time_code = v["@time"]
        cat02_code = v["@cat02"]
        value_str = v.get("$", v.get("#text", None))
        if value_str in (None, "", "-"):
            continue

        try:
            value = float(value_str)
        except ValueError:
            continue

        if cat02_code not in cat02_map:
            continue

        info = cat02_map[cat02_code]

        rows.append({
            "year": int(time_code[:4]),
            "month": info["month"],
            "hs10": format_hs10(hs10),
            "area_code": area_code,
            "area_name": area_map.get(area_code, area_code),
            "var": info["var"],
            "value": value,
        })

    return pd.DataFrame(rows)

# ================================================================
# 3. MAIN DOWNLOAD (EXPORT + IMPORT)
# ================================================================
df_all_flows = []

for flow, stats_id in TRADE_TYPES.items():
    print(f"\n=== 📦 Fetching {flow} metadata ===")
    meta = get_meta(stats_id)
    

    
    area_map, cat02_map, cat02_codes = extract_meta_maps(meta)

    all_years = []
    for y in YEARS:
        print(f"📡 Fetching JP {flow} data for {y}...")
        df_y = fetch_year(y, stats_id, area_map, cat02_map, cat02_codes)
        all_years.append(df_y)

    df_long = pd.concat(all_years, ignore_index=True)

    # Pivot NO / KG / Yen
    df_pivot = (
        df_long.pivot_table(
            index=["year", "month", "area_code", "area_name", "hs10"],
            columns="var",
            values="value",
            aggfunc="first"
        )
        .reset_index()
    )
    df_pivot.columns.name = None

    df_pivot = df_pivot.rename(columns={"NO": "NO", "KG": "KG", "YEN": "Yen_thousand"})
    df_pivot["yyyymm"] = df_pivot["year"].astype(str) + df_pivot["month"].astype(str).str.zfill(2)

    df_pivot["flow"] = flow
    df_all_flows.append(df_pivot)

# Combine EXPORT + IMPORT
df_final = pd.concat(df_all_flows, ignore_index=True)

df_final = df_final[
    [
        "flow",
        "yyyymm",
        "year",
        "month",
        "area_code",
        "area_name",
        "hs10",
        "NO",
        "KG",
        "Yen_thousand",
    ]
].sort_values(["flow", "year", "month", "area_code", "hs10"])

# ================================================================
# 4. VALIDATION CHECKS
# ================================================================
print("\n🔍 Running validation checks...")

required_cols = [
    "flow","yyyymm","year","month","area_code","area_name",
    "hs10","NO","KG","Yen_thousand"
]

missing_cols = [c for c in required_cols if c not in df_final.columns]
if missing_cols:
    print("❌ Missing required columns:", missing_cols)
    sys.exit(1)

if df_final.empty:
    print("❌ ERROR: Combined dataset is empty.")
    sys.exit(1)

bad_yyyymm = df_final[~df_final["yyyymm"].str.match(r"^\d{6}$")]
if not bad_yyyymm.empty:
    print("❌ Invalid yyyymm values found:")
    print(bad_yyyymm.head())
    sys.exit(1)

print("✅ Validation passed.")

# ================================================================
# 5. SAVE FINAL OUTPUT
# ================================================================
print(f"\n💾 Saving JP combined trade file to {OUTPUT_FILE}")
df_final.to_csv(OUTPUT_FILE, index=False, encoding=ENC)

print("✅ JP trade CSV saved.")

# ================================================================
# 6. AUTO-RUN VISUALIZATION PIPELINE
# ================================================================
print("\n🚀 Running jp_full_hs10_plot_script.py ...")

try:
    result = subprocess.run(
        ["python", str(EXPORT_DIR / "jp_full_hs10_plot_script.py")],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    print(result.stderr)
except Exception as e:
    print("❌ Failed to run jp_full_hs10_plot_script.py")
    print(e)
    sys.exit(1)

print("🎉 All JP plots generated successfully.")
