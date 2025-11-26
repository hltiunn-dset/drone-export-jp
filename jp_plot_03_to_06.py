#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
JP Version of TW plot_03_to_06.py
Replicates Taiwan’s structure 1:1, producing JP-prefixed files only.
"""

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import re
from pathlib import Path
import calendar as _cal

# ================================================================
# 1. JP Export Directory
# ================================================================
DATA_DIR = Path(
    "~/Library/Mobile Documents/com~apple~CloudDocs/github/drone-export-jp"
).expanduser()
EXPORT_DIR = Path(
    "~/Library/Mobile Documents/com~apple~CloudDocs/github/drone-export-jp/plot"
).expanduser()
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

# Load JP cleaned data produced by jp_full_hs10_plot_script_v2.py
df = pd.read_csv(DATA_DIR / "JP_cleaned_export_by_hs10.csv")

# ================================================================
# 2. Country Color Map (same logic as TW)
# ================================================================
cmap = plt.get_cmap("tab20")
all_countries = sorted(df["country"].dropna().unique())
country_color_map = {c: cmap(i % cmap.N) for i, c in enumerate(all_countries)}

# ================================================================
# 3. Quarter sorting logic (same as TW)
# ================================================================
def _qtr_sort_key(lbl: str):
    m = re.match(r"(\d{4})\s+Q([1-4])$", lbl)
    if m:
        return (int(m.group(1)), int(m.group(2)), 0)

    # Running quarter style (YYYY Mon/Mon/...)
    m = re.match(r"(\d{4})\s+([A-Za-z]{3})(?:/([A-Za-z]{3})(?:/([A-Za-z]{3}))?)?$", lbl)
    if m:
        year = int(m.group(1))
        first_mon = m.group(2)
        mon_num = list(_cal.month_abbr).index(first_mon.capitalize())
        q = (mon_num - 1) // 3 + 1
        return (year, q, 1)

    return (0, 0, 0)

def _sort_pivot_index(pivot: pd.DataFrame) -> pd.DataFrame:
    order = sorted(pivot.index.tolist(), key=_qtr_sort_key)
    return pivot.reindex(order)

# ================================================================
# 4. Cleaning for filenames (identical to TW)
# ================================================================
def _clean_val_for_filename(val: str) -> str:
    s = str(val).strip()
    s = s.replace(" ", "_")
    s = s.replace("/", "_")
    s = s.replace(".", "")
    s = s.replace(":", "_")
    s = "".join(ch for ch in s if ch not in "<>\"|?*")
    return s or "Unknown"

# ================================================================
# 5. Stacked plot helper (TW structure)
# ================================================================
def _stacked_plot(pivot, title, ylabel, filename, fmt="{val}", label_thresh=10):
    fig, ax = plt.subplots(figsize=(12, 6))

    col_totals = pivot.sum(axis=0)
    all_below = (col_totals <= label_thresh).all()

    visible_countries = [
        c for c in pivot.columns
        if (col_totals[c] > label_thresh or all_below) and col_totals[c] > 0
    ]
    pivot = pivot[visible_countries]
    colors = [country_color_map.get(c, "#CCCCCC") for c in pivot.columns]

    pivot.plot(kind="bar", stacked=True, ax=ax, color=colors)

    max_val = pivot.values.max()
    show_all = max_val <= label_thresh

    for i, (idx, row) in enumerate(pivot.iterrows()):
        cumulative = 0
        for j, val in enumerate(row):
            if val == 0:
                continue
            if show_all or val > label_thresh:
                ax.text(i, cumulative + val/2,
                        fmt.format(val=val),
                        ha="center", va="center", fontsize=8)
            cumulative += val

    ax.set_title(title)
    ax.set_ylabel(ylabel)
    ax.set_xlabel("Quarter/Period")

    if visible_countries:
        ax.legend(title="Country", bbox_to_anchor=(1.05, 1), loc='upper left')
    else:
        ax.legend().set_visible(False)

    plt.tight_layout()
    outpath = EXPORT_DIR / filename
    fig.savefig(outpath, dpi=300)
    plt.close(fig)
    return outpath

# ================================================================
# 6. PLOT 03 & 04 (value by quarter / percent share)
# ================================================================
def run_plot_03_04(category_col):
    if category_col not in df.columns:
        print(f"❌ Column not found: {category_col}")
        return

    subsets = {
        "All": df,
        "Exclude_re-export": df[~df["is_reexport"]]
    }

    for suffix, subset in subsets.items():

        # --------------------- Plot 03: VALUE ---------------------
        total_value = subset.groupby(["qtr", "country"])["K JPY"].sum().reset_index()
        pivot_value = total_value.pivot(index="qtr", columns="country",
                                        values="K JPY").fillna(0)

        pivot_value = pivot_value[pivot_value.sum().sort_values(ascending=False).index]
        pivot_value = _sort_pivot_index(pivot_value)

        f03 = f"JP_03_Value_{suffix}.png"
        t03 = f"Total Export Value by Country – {suffix}"

        _stacked_plot(
            pivot_value, t03, "Value (K JPY)",
            f03, fmt="{val:.0f}", label_thresh=1000
        )

        # --------------------- Plot 04: PERCENT VALUE SHARE ---------------------
        pct_value = pivot_value.div(pivot_value.sum(axis=1), axis=0) * 100

        f04 = f"JP_04_ValuePct_{suffix}.png"
        t04 = f"Value Share by Country – {suffix}"

        _stacked_plot(
            pct_value, t04, "Percentage of Quarter Value (%)",
            f04, fmt="{val:.1f}%", label_thresh=2
        )

        # --------------------- Per Category ---------------------
        grouped = subset.groupby(["qtr", category_col, "country"])["K JPY"].sum().reset_index()

        for cat_val in grouped[category_col].dropna().unique():
            cat_df = grouped[grouped[category_col] == cat_val]
            pivot = cat_df.pivot(index="qtr", columns="country", values="K JPY").fillna(0)

            pivot = pivot[pivot.sum().sort_values(ascending=False).index]
            pivot = _sort_pivot_index(pivot)

            clean_val = _clean_val_for_filename(cat_val)

            f03c = f"JP_03_Value_{category_col}_{suffix}_{clean_val}.png"
            f04c = f"JP_04_ValuePct_{category_col}_{suffix}_{clean_val}.png"

            t03c = f"Export Value by Country for {cat_val} – {suffix}"
            t04c = f"Value Share for {cat_val} – {suffix}"

            # MTOW annotation
            if category_col == "hs10":
                mtow_val = df[df["hs10"] == cat_val]["MTOW"].dropna().unique()
                if len(mtow_val) == 1:
                    t03c += f" (MTOW: {mtow_val[0]})"
                    t04c += f" (MTOW: {mtow_val[0]})"

            _stacked_plot(
                pivot, t03c, "Value (K JPY)",
                f03c, fmt="{val:.0f}", label_thresh=1000
            )

            pct = pivot.div(pivot.sum(axis=1), axis=0) * 100
            _stacked_plot(
                pct, t04c, "Percentage of Quarter Value (%)",
                f04c, fmt="{val:.1f}%", label_thresh=2
            )

    print(f"✅ JP plot_03_04 finished for {category_col}")

# ================================================================
# 7. PLOT 05 & 06 (period-based: Units + Value)
#    Mirrors TW logic (period labels from JP cleaning)
# ================================================================
def run_plot_05_06(category_col):
    if category_col not in df.columns:
        print(f"❌ Column not found: {category_col}")
        return

    subsets = {
        "All": df,
        "Exclude_re-export": df[~df["is_reexport"]]
    }

    for suffix, subset in subsets.items():
        # ------------------------------ UNITS ------------------------------
        total_units = subset.groupby(["period", "country"])["Quanity"].sum().reset_index()
        p_units = total_units.pivot(index="period", columns="country", values="Quanity").fillna(0)

        p_units = p_units[p_units.sum().sort_values(ascending=False).index]

        f05 = f"JP_05_Value_Total_{suffix}.png"
        t05 = f"Total Export Units by Period – {suffix}"

        _stacked_plot(
            p_units, t05, "Units (NO)",
            f05, fmt="{val:.0f}", label_thresh=10
        )

        # ------------------------------ VALUE ------------------------------
        total_val = subset.groupby(["period", "country"])["K JPY"].sum().reset_index()
        p_val = total_val.pivot(index="period", columns="country", values="K JPY").fillna(0)

        p_val = p_val[p_val.sum().sort_values(ascending=False).index]

        f05_val = f"JP_05_Value_Total_Yen_{suffix}.png"
        t05_val = f"Total Export Value by Period – {suffix}"

        _stacked_plot(
            p_val, t05_val, "Value (K JPY)",
            f05_val, fmt="{val:.0f}", label_thresh=500
        )

        # ------------------------------ PERCENT UNITS ------------------------------
        pct_units = p_units.div(p_units.sum(axis=1), axis=0) * 100

        f06 = f"JP_06_ValuePct_Total_{suffix}.png"
        t06 = f"Unit Share by Period – {suffix}"

        _stacked_plot(
            pct_units, t06, "Percentage Share (%)",
            f06, fmt="{val:.1f}%", label_thresh=2
        )

        # ------------------------------ PERCENT VALUE ------------------------------
        pct_val = p_val.div(p_val.sum(axis=1), axis=0) * 100

        f06_val = f"JP_06_ValuePct_Total_Yen_{suffix}.png"
        t06_val = f"Value Share by Period – {suffix}"

        _stacked_plot(
            pct_val, t06_val, "Percentage Share (%)",
            f06_val, fmt="{val:.1f}%", label_thresh=2
        )

    print(f"✅ JP plot_05_06 finished for {category_col}")
