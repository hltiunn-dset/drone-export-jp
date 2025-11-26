#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
JP Export Downloader + Auto-Plot Pipeline Runner
------------------------------------------------
1. Downloads Japan HS10=8806 export data from e-Stat API
2. Validates dataset structure
3. Saves cleaned CSV into drone-export-jp folder
4. Automatically triggers:
      jp_full_hs10_plot_script_v2.py
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
STATS_ID = "0003425293"
BASE_URL_META = "https://api.e-stat.go.jp/rest/3.0/app/json/getMetaInfo"
BASE_URL_DATA = "https://api.e-stat.go.jp/rest/3.0/app/json/getStatsData"

YEARS = ["2022", "2023", "2024", "2025"]

EXPORT_DIR = Path(
    "~/Library/Mobile Documents/com~apple~CloudDocs/github/drone-export-jp"
).expanduser()
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = EXPORT_DIR / "jp_8806_by_country_month_2023_2025.csv"

ENC = "utf-8-sig"

# ================================================================
# 1. FETCH METADATA
# ================================================================
def get_meta():
    params = {
        "appId": APP_ID,
        "statsDataId": STATS_ID,
        "lang": "J",
    }
    r = requests.get(BASE_URL_META, params=params)
    r.raise_for_status()
    return r.json()

print("📡 Fetching metadata...")
meta = get_meta()
class_objs = meta["GET_META_INFO"]["METADATA_INF"]["CLASS_INF"]["CLASS_OBJ"]

def get_class_obj(class_id):
    for obj in class_objs:
        if obj["@id"] == class_id:
            return obj
    raise ValueError(f"Class {class_id} not found")

area_obj = get_class_obj("area")
cat02_obj = get_class_obj("cat02")

AREA_MAP = {c["@code"]: c["@name"] for c in area_obj["CLASS"]}

# Parse cat02 → (month, var)
CAT02_MONTH_MAP = {}
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
        CAT02_MONTH_MAP[code] = {"month": month, "var": var}

CAT02_CODES = sorted(CAT02_MONTH_MAP.keys(), key=int)

# HS10 list under 8806
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
# 2. FETCH YEAR DATA
# ================================================================
def fetch_year(year):
    time_code = f"{year}000000"
    params = {
        "appId": APP_ID,
        "statsDataId": STATS_ID,
        "lang": "J",
        "cdTime": time_code,
        "cdCat01": ",".join(HS10_LIST),
        "cdCat02": ",".join(CAT02_CODES),
        "limit": 100000,
    }
    r = requests.get(BASE_URL_DATA, params=params)
    r.raise_for_status()
    js = r.json()

    try:
        values = js["GET_STATS_DATA"]["STATISTICAL_DATA"]["DATA_INF"]["VALUE"]
    except KeyError:
        raise RuntimeError(f"No VALUE found for {year}")

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

        if cat02_code not in CAT02_MONTH_MAP:
            continue

        info = CAT02_MONTH_MAP[cat02_code]

        rows.append({
            "year": int(time_code[:4]),
            "month": info["month"],
            "hs10": format_hs10(hs10),
            "area_code": area_code,
            "area_name": AREA_MAP.get(area_code, area_code),
            "var": info["var"],
            "value": value,
        })

    return pd.DataFrame(rows)

# ================================================================
# 3. DOWNLOAD ALL YEARS
# ================================================================
all_years = []
for y in YEARS:
    print(f"📡 Fetching JP export data for {y}...")
    df_y = fetch_year(y)
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

df_pivot["yyyymm"] = (
    df_pivot["year"].astype(str) + df_pivot["month"].astype(str).str.zfill(2)
)

df_final = df_pivot[
    [
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
].sort_values(["year", "month", "area_code", "hs10"])

# ================================================================
# 4. VALIDATION CHECKS
# ================================================================
print("🔍 Running validation checks...")

required_cols = [
    "yyyymm","year","month","area_code","area_name",
    "hs10","NO","KG","Yen_thousand"
]

missing_cols = [c for c in required_cols if c not in df_final.columns]
if missing_cols:
    print("❌ Missing required columns:", missing_cols)
    sys.exit(1)

if df_final.empty:
    print("❌ ERROR: Downloaded dataset is empty.")
    sys.exit(1)

# yyyymm validity
bad_yyyymm = df_final[~df_final["yyyymm"].str.match(r"^\d{6}$")]
if not bad_yyyymm.empty:
    print("❌ Invalid yyyymm values found.")
    print(bad_yyyymm.head())
    sys.exit(1)

print("✅ Validation passed.")

# ================================================================
# 5. SAVE OUTPUT FILE
# ================================================================
print(f"💾 Saving JP export file to {OUTPUT_FILE}")
df_final.to_csv(OUTPUT_FILE, index=False, encoding=ENC)

print("✅ JP raw CSV saved.")

# ================================================================
# 6. AUTO-RUN THE JP VISUALIZATION PIPELINE
# ================================================================
print("🚀 Running jp_full_hs10_plot_script.py ...")

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
