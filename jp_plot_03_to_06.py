#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
JP Version of TW plot_03_to_06.py
Updated for combined IMPORT + EXPORT pipeline.

Usage:
    python jp_plot_03_to_06.py EXPORT
    python jp_plot_03_to_06.py IMPORT
"""

import sys
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import re
from pathlib import Path
import calendar as _cal

# ================================================================
# 0. FLOW ARGUMENT (EXPORT / IMPORT)
# ================================================================
if len(sys.argv) < 2:
    print("❌ Missing flow argument. Use: python jp_plot_03_to_06.py EXPORT")
    sys.exit(1)

FLOW = sys.argv[1].upper()
if FLOW not in ("EXPORT", "IMPORT"):
    print("❌ Invalid argument. Use EXPORT or IMPORT.")
    sys.exit(1)

PREFIX = f"JP_{FLOW}"

# ================================================================
# 1. Directories
# ================================================================
DATA_DIR = Path(
    "~/Library/Mobile Documents/com~apple~CloudDocs/github/drone-export-jp"
).expanduser()

EXPORT_DIR = DATA_DIR / "plot"
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

# Load correct dataset
CLEAN_FILE = DATA_DIR / f"JP_cleaned_{FLOW.lower()}_by_hs10.csv"
print(f"📂 Loading dataset: {CLEAN_FILE}")

df = pd.read_csv(CLEAN_FILE)

# ================================================================
# 2. Country Color Map
# ================================================================
cmap = plt.get_cmap("tab20")
all_countries = sorted(df["country"].dropna().unique())
country_color_map = {c: cmap(i % cmap.N) for i, c in enumerate(all_countries)}

# ================================================================
# 3. Quarter Sorting (TW logic)
# ================================================================
def _qtr_sort_key(lbl: str):
    # YYYY Qn
    m = re.match(r"(\d{4})\s+Q([1-4])$", lbl)
    if m:
        return (int(m.group(1)), int(m.group(2)), 0)

    # Running quarterly: YYYY Jan/Feb/Mar
    m = re.match(r"(\d{4})\s+([A-Za-z]{3})(?:/([A-Za-z]{3})"
                 r"(?:/([A-Za-z]{3}))?)?$", lbl)
    if m:
        year = int(m.group(1))
        mon = list(_cal.month_abbr).index(m.group(2).capitalize())
        q = (mon - 1) // 3 + 1
        return (year, q, 1)

    return (0, 0, 0)

def _sort_pivot_index(pivot):
    order = sorted(pivot.index.tolist(), key=_qtr_sort_key)
    return pivot.reindex(order)

# ================================================================
# 4. Filename cleaner
# ================================================================
def _clean_val_for_filename(v):
    s = str(v).strip()
    s = s.replace(" ", "_").replace("/", "_").replace(":", "_")
    s = s.replace(".", "")
    s = "".join(ch for ch in s if ch not in "<>\"|?*")
    return s or "Unknown"

# ================================================================
# 5. Stacked Plot Helper
# ================================================================
def _stacked_plot(pivot, title, ylabel, filename, fmt="{val}", label_thresh=10):
    fig, ax = plt.subplots(figsize=(12, 6))

    col_totals = pivot.sum(axis=0)
    all_below = (col_totals <= label_thresh).all()

    visible = [
        c for c in pivot.columns
        if (col_totals[c] > label_thresh or all_below) and col_totals[c] > 0
    ]

    pivot = pivot[visible]
    colors = [country_color_map.get(c, "#CCCCCC") for c in pivot.columns]
    pivot.plot(kind="bar", stacked=True, ax=ax, color=colors)

    max_val = pivot.values.max()
    show_all = max_val <= label_thresh

    # Label each bar segment
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
    ax.set_xlabel("Quarter / Period")

    if visible:
        ax.legend(title="Country", bbox_to_anchor=(1.05, 1), loc="upper left")
    else:
        ax.legend().set_visible(False)

    plt.tight_layout()
    fig.savefig(EXPORT_DIR / filename, dpi=300)
    plt.close(fig)
    return filename

# ================================================================
# 6. Plot 03 & 04 (Value by Quarter)
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

        # --------------------- Plot 03: TOTAL VALUE ---------------------
        total_val = subset.groupby(["qtr", "country"])["K JPY"].sum().reset_index()
        pivot_val = total_val.pivot(index="qtr", columns="country", values="K JPY").fillna(0)

        pivot_val = pivot_val[pivot_val.sum().sort_values(ascending=False).index]
        pivot_val = _sort_pivot_index(pivot_val)

        f03 = f"{PREFIX}_03_Value_{suffix}.png"
        t03 = f"{FLOW} Value by Country – {suffix}"

        _stacked_plot(
            pivot_val, t03, "Value (K JPY)",
            f03, fmt="{val:.0f}", label_thresh=1000
        )

        # --------------------- Plot 04: PERCENT VALUE SHARE ---------------------
        pct_val = pivot_val.div(pivot_val.sum(axis=1), axis=0) * 100

        f04 = f"{PREFIX}_04_ValuePct_{suffix}.png"
        t04 = f"{FLOW} Value Share – {suffix}"

        _stacked_plot(
            pct_val, t04, "Percentage Value (%)",
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

            f03c = f"{PREFIX}_03_Value_{category_col}_{suffix}_{clean_val}.png"
            f04c = f"{PREFIX}_04_ValuePct_{category_col}_{suffix}_{clean_val}.png"

            title03 = f"{FLOW} Value for {cat_val} – {suffix}"
            title04 = f"{FLOW} Value Share for {cat_val} – {suffix}"

            # Add MTOW annotation
            if category_col == "hs10":
                mtow = df[df["hs10"] == cat_val]["MTOW"].dropna().unique()
                if len(mtow) == 1:
                    title03 += f" (MTOW: {mtow[0]})"
                    title04 += f" (MTOW: {mtow[0]})"

            _stacked_plot(
                pivot, title03, "Value (K JPY)",
                f03c, fmt="{val:.0f}", label_thresh=1000
            )

            pct = pivot.div(pivot.sum(axis=1), axis=0) * 100

            _stacked_plot(
                pct, title04, "Percentage Value (%)",
                f04c, fmt="{val:.1f}%", label_thresh=2
            )

    print(f"✅ Finished JP plot_03_04 for {category_col} ({FLOW})")

# ================================================================
# 7. Plot 05 & 06 (Period-based Units + Value)
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

        # ------------------------------ PERIOD UNITS ------------------------------
        total_units = subset.groupby(["period", "country"])["Quanity"].sum().reset_index()
        p_units = total_units.pivot(index="period", columns="country",
                                    values="Quanity").fillna(0)

        p_units = p_units[p_units.sum().sort_values(ascending=False).index]

        f05 = f"{PREFIX}_05_Units_{suffix}.png"
        t05 = f"{FLOW} Units by Period – {suffix}"

        _stacked_plot(
            p_units, t05, "Units (NO)",
            f05, fmt="{val:.0f}", label_thresh=10
        )

        # ------------------------------ PERIOD VALUE ------------------------------
        total_val = subset.groupby(["period", "country"])["K JPY"].sum().reset_index()
        p_val = total_val.pivot(index="period", columns="country",
                                values="K JPY").fillna(0)

        p_val = p_val[p_val.sum().sort_values(ascending=False).index]

        f05v = f"{PREFIX}_05_Value_{suffix}.png"
        t05v = f"{FLOW} Value by Period – {suffix}"

        _stacked_plot(
            p_val, t05v, "Value (K JPY)",
            f05v, fmt="{val:.0f}", label_thresh=500
        )

        # ------------------------------ PERCENT UNITS ------------------------------
        pct_units = p_units.div(p_units.sum(axis=1), axis=0) * 100

        f06 = f"{PREFIX}_06_UnitsPct_{suffix}.png"
        t06 = f"{FLOW} Unit Share by Period – {suffix}"

        _stacked_plot(
            pct_units, t06, "Share (%)",
            f06, fmt="{val:.1f}%", label_thresh=2
        )

        # ------------------------------ PERCENT VALUE ------------------------------
        pct_val = p_val.div(p_val.sum(axis=1), axis=0) * 100

        f06v = f"{PREFIX}_06_ValuePct_{suffix}.png"
        t06v = f"{FLOW} Value Share by Period – {suffix}"

        _stacked_plot(
            pct_val, t06v, "Share (%)",
            f06v, fmt="{val:.1f}%", label_thresh=2
        )

    print(f"✅ Finished JP plot_05_06 for {category_col} ({FLOW})")


# If executed directly by subprocess (TW-style)
if __name__ == "__main__":
    run_plot_03_04("hs10")
    run_plot_03_04("US_Group")
    run_plot_03_04("NATO_Class")

    run_plot_05_06("hs10")
    run_plot_05_06("US_Group")
    run_plot_05_06("NATO_Class")

