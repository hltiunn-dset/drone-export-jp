#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
JP Version of TW plot_01_to_02.py
Exact same structure, naming logic, and plot logic,
but adapted for JP data fields and JP output directory.
"""

import matplotlib.pyplot as plt
import pandas as pd
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

# Load JP cleaned data (produced by jp_full_hs10_plot_script_v2.py)
df = pd.read_csv(DATA_DIR / "JP_cleaned_export_by_hs10.csv")

# ================================================================
# 2. Build Country Color Map (same as TW)
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

    # Running quarter format (YYYY Mon/Mon...)
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
# 4. Stacked Bar Plot (same code as TW)
# ================================================================
def stacked_plot(pivot, title, ylabel, filename, fmt="{val}", label_thresh=10):
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

    # Labels
    max_val = pivot.values.max()
    show_all = max_val <= label_thresh

    for i, (idx, row) in enumerate(pivot.iterrows()):
        cumulative = 0
        for j, val in enumerate(row):
            if val == 0:
                continue
            if show_all or val > label_thresh:
                text = fmt.format(val=val)
                ax.text(i, cumulative + val / 2, text, ha="center", va="center", fontsize=8)
            cumulative += val

    ax.set_title(title)
    ax.set_ylabel(ylabel)
    ax.set_xlabel("Quarter/Month")

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
# 5. Main function (identical to TW version)
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
        # 01 – TOTAL COUNTS by Quarter and Country
        # ====================================================
        total = subset.groupby(["qtr", "country"])["Quanity"].sum().reset_index()
        pivot_total = total.pivot(index="qtr", columns="country", values="Quanity").fillna(0)

        pivot_total = pivot_total[pivot_total.sum().sort_values(ascending=False).index]
        pivot_total = _sort_pivot_index(pivot_total)

        # JP prefix added here
        f1 = f"JP_01_Counts_total_{suffix}.png"
        t1 = f"Total Drone Exports by Country – {suffix}"

        stacked_plot(
            pivot_total, t1, "Number of Drones Exported",
            f1, "{val:.0f}", 10
        )

        # ====================================================
        # 02 – TOTAL PERCENTAGE SHARE
        # ====================================================
        percent_total = pivot_total.div(pivot_total.sum(axis=1), axis=0) * 100

        f2 = f"JP_02_Percent_total_{suffix}.png"
        t2 = f"Total Export Share by Country – {suffix}"

        stacked_plot(
            percent_total, t2,
            "Percentage of Quarter Total (%)",
            f2, "{val:.1f}%", 1.5
        )

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

            f3 = f"JP_01_Counts_{category_col}_{suffix}_{clean_name}.png"
            f4 = f"JP_02_Percent_{category_col}_{suffix}_{clean_name}.png"

            title1 = f"Drone Exports by Country for {cat_val} – {suffix}"
            title2 = f"Export Share by Country for {cat_val} – {suffix}"

            # MTOW annotation for HS10 category
            if category_col == "hs10":
                mtow_val = df[df["hs10"] == cat_val]["MTOW"].dropna().unique()
                if len(mtow_val) == 1:
                    title1 += f" (MTOW: {mtow_val[0]})"
                    title2 += f" (MTOW: {mtow_val[0]})"

            stacked_plot(
                pivot, title1,
                "Number of Drones Exported",
                f3, "{val:.0f}", 10
            )

            percent = pivot.div(pivot.sum(axis=1), axis=0) * 100

            stacked_plot(
                percent, title2,
                "Percentage of Quarter Total (%)",
                f4, "{val:.1f}%", 1.5
            )

    print(f"✅ JP plot_01_02 finished for {category_col}")
