#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
JP Version of TW plot_01_to_02.py
Updated to match combined IMPORT + EXPORT pipeline.

This script:
 - Accepts flow argument: "EXPORT" or "IMPORT"
 - Loads:
       JP_cleaned_export_by_hs10.csv
       or
       JP_cleaned_import_by_hs10.csv
 - Outputs PNGs under /plot/ with JP_EXPORT_ or JP_IMPORT_ prefixes
"""

import sys
import matplotlib.pyplot as plt
import pandas as pd
import re
from pathlib import Path
import calendar as _cal

# ================================================================
# 0. Validate flow argument (EXPORT or IMPORT)
# ================================================================
if len(sys.argv) < 2:
    print("❌ Missing flow argument. Use: python jp_plot_01_to_02.py EXPORT")
    sys.exit(1)

FLOW = sys.argv[1].upper()
if FLOW not in ("EXPORT", "IMPORT"):
    print("❌ Invalid flow argument. Use EXPORT or IMPORT.")
    sys.exit(1)

# Prefix for output files
PREFIX = f"JP_{FLOW}"

# ================================================================
# 1. Directories
# ================================================================
DATA_DIR = Path(
    "~/Library/Mobile Documents/com~apple~CloudDocs/github/drone-export-jp"
).expanduser()

EXPORT_DIR = DATA_DIR / "plot"
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

# Load respective dataset
CLEAN_FILE = DATA_DIR / f"JP_cleaned_{FLOW.lower()}_by_hs10.csv"

print(f"📂 Loading dataset: {CLEAN_FILE}")
df = pd.read_csv(CLEAN_FILE)

# ================================================================
# 2. Country Colors
# ================================================================
cmap = plt.get_cmap("tab20")
all_countries = sorted(df["country"].dropna().unique())
country_color_map = {c: cmap(i % cmap.N) for i, c in enumerate(all_countries)}

# ================================================================
# 3. Quarter Sort Logic (identical to TW)
# ================================================================
def _qtr_sort_key(lbl: str):
    # YYYY Qn
    m = re.match(r"(\d{4})\s+Q([1-4])$", lbl)
    if m:
        return (int(m.group(1)), int(m.group(2)), 0)

    # Running month range
    m = re.match(r"(\d{4})\s+([A-Za-z]{3})(?:/([A-Za-z]{3})(?:/([A-Za-z]{3}))?)?$", lbl)
    if m:
        year = int(m.group(1))
        mon = list(_cal.month_abbr).index(m.group(2).capitalize())
        q = (mon - 1) // 3 + 1
        return (year, q, 1)

    return (0, 0, 0)

def _sort_pivot_index(pivot: pd.DataFrame):
    order = sorted(pivot.index.tolist(), key=_qtr_sort_key)
    return pivot.reindex(order)

# ================================================================
# 4. Generic stacked bar plot
# ================================================================
def stacked_plot(pivot, title, ylabel, filename, fmt="{val}", label_thresh=10):
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

    # Labels
    max_val = pivot.values.max()
    show_all = max_val <= label_thresh

    for i, (idx, row) in enumerate(pivot.iterrows()):
        cumulative = 0
        for j, val in enumerate(row):
            if val == 0:
                continue
            if show_all or val > label_thresh:
                ax.text(i, cumulative + val/2, fmt.format(val=val),
                        ha="center", va="center", fontsize=8)
            cumulative += val

    ax.set_title(title)
    ax.set_ylabel(ylabel)
    ax.set_xlabel("Quarter/Month")

    if visible:
        ax.legend(title="Country", bbox_to_anchor=(1.05, 1), loc="upper left")
    else:
        ax.legend().set_visible(False)

    plt.tight_layout()
    fig.savefig(EXPORT_DIR / filename, dpi=300)
    plt.close(fig)

# ================================================================
# 5. Main function
# ================================================================
def run_plot_01_02(category_col):
    if category_col not in df.columns:
        print(f"❌ Column not found: {category_col}")
        return

    subsets = {
        "All": df,
        "Exclude_re-export": df[~df["is_reexport"]]
    }

    for suffix, subset in subsets.items():

        # ====================================================
        # 01 — Total Counts
        # ====================================================
        total = subset.groupby(["qtr", "country"])["Quanity"].sum().reset_index()
        pivot_total = total.pivot(index="qtr", columns="country", values="Quanity").fillna(0)

        pivot_total = pivot_total[pivot_total.sum().sort_values(ascending=False).index]
        pivot_total = _sort_pivot_index(pivot_total)

        f1 = f"{PREFIX}_01_Counts_total_{suffix}.png"
        t1 = f"{FLOW} – Total by Country ({suffix})"

        stacked_plot(pivot_total, t1, "Quantity", f1, "{val:.0f}", 10)

        # ====================================================
        # 02 — Percent Share
        # ====================================================
        percent_total = pivot_total.div(pivot_total.sum(axis=1), axis=0) * 100

        f2 = f"{PREFIX}_02_Percent_total_{suffix}.png"
        t2 = f"{FLOW} – Country Share ({suffix})"

        stacked_plot(percent_total, t2, "Share (%)", f2, "{val:.1f}%", 1.5)

        # ====================================================
        # Per Category (HS10, US_Group, NATO_Class)
        # ====================================================
        grouped = subset.groupby(["qtr", category_col, "country"])["Quanity"].sum().reset_index()

        for cat_val in grouped[category_col].dropna().unique():
            cat_df = grouped[grouped[category_col] == cat_val]
            pivot = cat_df.pivot(index="qtr", columns="country", values="Quanity").fillna(0)

            pivot = pivot[pivot.sum().sort_values(ascending=False).index]
            pivot = _sort_pivot_index(pivot)

            clean_name = str(cat_val).replace(" ", "_").replace("/", "_").replace(".", "")

            f3 = f"{PREFIX}_01_Counts_{category_col}_{suffix}_{clean_name}.png"
            f4 = f"{PREFIX}_02_Percent_{category_col}_{suffix}_{clean_name}.png"

            title1 = f"{FLOW} – {category_col}: {cat_val} ({suffix})"
            title2 = f"{FLOW} Share – {category_col}: {cat_val} ({suffix})"

            # Add MTOW annotation if HS10
            if category_col == "hs10":
                mtow = df[df["hs10"] == cat_val]["MTOW"].dropna().unique()
                if len(mtow) == 1:
                    title1 += f" (MTOW: {mtow[0]})"
                    title2 += f" (MTOW: {mtow[0]})"

            stacked_plot(pivot, title1, "Quantity", f3, "{val:.0f}", 10)

            percent = pivot.div(pivot.sum(axis=1), axis=0) * 100

            stacked_plot(percent, title2, "Share (%)", f4, "{val:.1f}%", 1.5)

    print(f"✅ Finished JP plot_01_02 for {category_col} ({FLOW})")


# If called directly by subprocess
if __name__ == "__main__":
    # These three match TW version
    run_plot_01_02("hs10")
    run_plot_01_02("US_Group")
    run_plot_01_02("NATO_Class")

