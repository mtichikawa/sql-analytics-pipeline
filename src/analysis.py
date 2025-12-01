"""
analysis.py — High-level analysis and visualization generation.

Runs the full analytical suite and produces a set of publication-quality
matplotlib/seaborn figures saved to data/outputs/.

Usage:
    python src/analysis.py
"""

import logging
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # non-interactive backend for script use
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import seaborn as sns
from scipy import stats

sys.path.insert(0, str(Path(__file__).parent.parent))
import config
from src.query_engine import QueryEngine

log = logging.getLogger("analysis")

# ── Style ──────────────────────────────────────────────────────────────────────
PALETTE = {
    "blue":   "#2C7BB6",
    "orange": "#D7191C",
    "green":  "#1A9641",
    "purple": "#7B2D8B",
    "gray":   "#636363",
    "light":  "#F7F7F7",
}

BOROUGH_COLORS = {
    "Manhattan":    "#2C7BB6",
    "Brooklyn":     "#1A9641",
    "Queens":       "#D7191C",
    "Staten Island":"#7B2D8B",
    "Bronx":        "#FF7F00",
}

def apply_style():
    plt.rcParams.update({
        "figure.facecolor":  "white",
        "axes.facecolor":    "#FAFAFA",
        "axes.grid":         True,
        "grid.alpha":        0.3,
        "grid.color":        "#CCCCCC",
        "font.family":       "DejaVu Sans",
        "font.size":         11,
        "axes.titlesize":    14,
        "axes.titleweight":  "bold",
        "axes.labelsize":    12,
        "xtick.labelsize":   10,
        "ytick.labelsize":   10,
        "legend.fontsize":   10,
        "figure.titlesize":  16,
        "figure.titleweight": "bold",
    })


# ── Fig 1: Borough price comparison ───────────────────────────────────────────
def fig_borough_comparison(qe: QueryEngine) -> str:
    df = qe.borough_comparison()
    df = df.dropna(subset=["borough"]).sort_values("avg_price", ascending=False)

    fig, axes = plt.subplots(1, 3, figsize=(16, 6))
    fig.suptitle("NYC Airbnb — Borough Comparison", y=1.02)

    colors = [BOROUGH_COLORS.get(b, "#888888") for b in df["borough"]]

    # avg price
    ax = axes[0]
    bars = ax.barh(df["borough"], df["avg_price"], color=colors, edgecolor="white", linewidth=0.5)
    ax.set_xlabel("Average Nightly Price ($)")
    ax.set_title("Average Price by Borough")
    ax.bar_label(bars, fmt="$%.0f", padding=3, fontsize=10)
    ax.set_xlim(0, df["avg_price"].max() * 1.25)

    # superhost pct
    ax = axes[1]
    bars = ax.barh(df["borough"], df["superhost_pct"], color=colors, edgecolor="white", linewidth=0.5)
    ax.set_xlabel("Superhost %")
    ax.set_title("Superhost Penetration by Borough")
    ax.bar_label(bars, fmt="%.1f%%", padding=3, fontsize=10)
    ax.set_xlim(0, df["superhost_pct"].max() * 1.25)

    # listing count
    ax = axes[2]
    bars = ax.barh(df["borough"], df["total_listings"], color=colors, edgecolor="white", linewidth=0.5)
    ax.set_xlabel("Total Listings")
    ax.set_title("Listing Count by Borough")
    ax.bar_label(bars, fmt="%d", padding=3, fontsize=10)
    ax.set_xlim(0, df["total_listings"].max() * 1.25)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}k"))

    plt.tight_layout()
    out = config.OUTPUTS_DIR / "fig1_borough_comparison.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    log.info(f"Saved {out}")
    return str(out)


# ── Fig 2: Price distribution by room type ────────────────────────────────────
def fig_price_distribution(qe: QueryEngine) -> str:
    """Violin + box plot of price by room type."""
    from sqlalchemy import text
    with qe.engine.connect() as conn:
        df = pd.read_sql(
            text("SELECT room_type, price FROM mart_listings WHERE price <= 600"),
            conn
        )

    fig, ax = plt.subplots(figsize=(12, 7))
    fig.suptitle("Price Distribution by Room Type")

    room_order = ["Entire home/apt", "Private room", "Hotel room", "Shared room"]
    room_order = [r for r in room_order if r in df["room_type"].unique()]
    palette = sns.color_palette("Set2", len(room_order))

    sns.violinplot(
        data=df, x="room_type", y="price",
        order=room_order, palette=palette,
        inner="quartile", cut=0, ax=ax
    )

    ax.set_xlabel("Room Type")
    ax.set_ylabel("Nightly Price ($)")
    ax.set_title("Price Distribution by Room Type (capped at $600)")

    # Add median labels
    for i, room in enumerate(room_order):
        median = df[df["room_type"] == room]["price"].median()
        ax.text(i, median + 5, f"  median\n  ${median:.0f}",
                ha="center", va="bottom", fontsize=9, color="black", fontweight="bold")

    plt.tight_layout()
    out = config.OUTPUTS_DIR / "fig2_price_distribution_room_type.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    log.info(f"Saved {out}")
    return str(out)


# ── Fig 3: Top neighborhoods heatmap ─────────────────────────────────────────
def fig_neighborhood_heatmap(qe: QueryEngine) -> str:
    df = qe.pricing_by_neighborhood(top_n=25)

    metrics = ["avg_price", "avg_rating", "superhost_pct", "avg_availability_365"]
    labels  = ["Avg Price ($)", "Avg Rating", "Superhost %", "Avg Avail (days/yr)"]

    heatmap_data = df.set_index("neighborhood")[metrics].copy()
    # Normalize each column to 0–1 for heatmap color scaling
    normalized = (heatmap_data - heatmap_data.min()) / (heatmap_data.max() - heatmap_data.min())

    fig, ax = plt.subplots(figsize=(10, 12))
    fig.suptitle("Top 25 NYC Neighborhoods — Key Metrics Heatmap")

    sns.heatmap(
        normalized, ax=ax,
        cmap="YlOrRd",
        xticklabels=labels,
        linewidths=0.5, linecolor="#DDDDDD",
        cbar_kws={"label": "Normalized score (0=lowest, 1=highest)"},
        annot=heatmap_data.round(1),
        fmt=".1f",
        annot_kws={"size": 8}
    )

    ax.set_xlabel("")
    ax.set_ylabel("")
    ax.set_title("Top 25 Neighborhoods by Average Price\n(values shown; color = normalized rank)")
    plt.xticks(rotation=20, ha="right")
    plt.yticks(rotation=0)
    plt.tight_layout()

    out = config.OUTPUTS_DIR / "fig3_neighborhood_heatmap.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    log.info(f"Saved {out}")
    return str(out)


# ── Fig 4: Superhost pricing premium ─────────────────────────────────────────
def fig_superhost_premium(qe: QueryEngine) -> str:
    df_super = qe.superhost_pricing_premium()
    df_multi = qe.multi_listing_hosts()

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle("Host Behavior Analysis")

    # Superhost vs regular
    ax = axes[0]
    colors = [PALETTE["blue"], PALETTE["orange"]]
    bars = ax.bar(df_super["host_type"], df_super["avg_price"],
                  color=colors, edgecolor="white", width=0.5)
    ax.bar_label(bars, fmt="$%.0f", padding=3, fontsize=12, fontweight="bold")
    ax.set_ylabel("Average Nightly Price ($)")
    ax.set_title("Superhost vs. Regular Host Pricing")
    ax.set_ylim(0, df_super["avg_price"].max() * 1.3)

    # Add rating annotations
    for i, row in df_super.iterrows():
        ax.text(i, 10, f"★ {row['avg_rating']:.2f}\n{row['listing_count']:,} listings",
                ha="center", va="bottom", fontsize=10, color="white", fontweight="bold")

    # Multi-listing host analysis
    ax = axes[1]
    scale_order = ["1 listing", "2–3 listings", "4–10 listings", "11–50 listings", "50+ listings"]
    df_multi = df_multi[df_multi["host_scale"].isin(scale_order)].copy()
    df_multi["host_scale"] = pd.Categorical(df_multi["host_scale"], categories=scale_order, ordered=True)
    df_multi = df_multi.sort_values("host_scale")

    palette = sns.color_palette("Blues_d", len(df_multi))
    bars = ax.bar(range(len(df_multi)), df_multi["avg_price"],
                  color=palette, edgecolor="white")
    ax.bar_label(bars, fmt="$%.0f", padding=3, fontsize=10)
    ax.set_xticks(range(len(df_multi)))
    ax.set_xticklabels(df_multi["host_scale"], rotation=20, ha="right")
    ax.set_ylabel("Average Listing Price ($)")
    ax.set_title("Pricing by Host Scale\n(# of listings managed)")

    plt.tight_layout()
    out = config.OUTPUTS_DIR / "fig4_host_analysis.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    log.info(f"Saved {out}")
    return str(out)


# ── Fig 5: Availability vs price ─────────────────────────────────────────────
def fig_availability_price(qe: QueryEngine) -> str:
    df = qe.price_vs_availability()

    order = ["highly_restricted", "restricted", "moderate", "available", "always_available"]
    labels = ["<30 days\n(very rare)", "30–90 days\n(restricted)",
              "90–180 days\n(moderate)", "180–270 days\n(available)", ">270 days\n(always on)"]
    df["availability_pattern"] = pd.Categorical(df["availability_pattern"], categories=order, ordered=True)
    df = df.sort_values("availability_pattern")

    fig, ax1 = plt.subplots(figsize=(12, 6))
    fig.suptitle("Availability Pattern vs. Pricing\n'Does scarcity drive premium prices?'")

    x = range(len(df))
    width = 0.6

    bars = ax1.bar(x, df["avg_price"], color=PALETTE["blue"], alpha=0.8,
                   width=width, label="Avg Price ($)")
    ax1.set_ylabel("Average Nightly Price ($)", color=PALETTE["blue"])
    ax1.tick_params(axis="y", labelcolor=PALETTE["blue"])
    ax1.set_xticks(x)
    ax1.set_xticklabels([labels[i] for i in range(len(df))], fontsize=10)
    ax1.bar_label(bars, fmt="$%.0f", padding=3, fontsize=10, color=PALETTE["blue"])

    ax2 = ax1.twinx()
    ax2.plot(x, df["avg_reviews_ltm"], color=PALETTE["orange"], marker="o",
             linewidth=2.5, markersize=8, label="Avg Reviews (LTM)")
    ax2.set_ylabel("Avg Reviews (last 12 months)", color=PALETTE["orange"])
    ax2.tick_params(axis="y", labelcolor=PALETTE["orange"])

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper right")
    ax1.set_xlabel("Listing Availability Pattern (days available per year)")

    plt.tight_layout()
    out = config.OUTPUTS_DIR / "fig5_availability_vs_price.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    log.info(f"Saved {out}")
    return str(out)


# ── Fig 6: Price × size matrix ────────────────────────────────────────────────
def fig_price_size_matrix(qe: QueryEngine) -> str:
    df = qe.price_by_size_and_room_type()

    pivot = df.pivot_table(index="size_category", columns="room_type",
                           values="avg_price", aggfunc="mean")

    size_order = ["intimate", "small", "medium", "large"]
    pivot = pivot.reindex([s for s in size_order if s in pivot.index])

    fig, ax = plt.subplots(figsize=(10, 6))
    fig.suptitle("Average Price: Size Category × Room Type")

    sns.heatmap(
        pivot, ax=ax, cmap="YlOrRd", annot=True, fmt=".0f",
        linewidths=0.5, linecolor="#DDDDDD",
        cbar_kws={"label": "Avg Nightly Price ($)"}
    )
    ax.set_xlabel("Room Type")
    ax.set_ylabel("Listing Size")
    ax.set_title("Price Matrix: Accommodation Size vs Room Type\n(values = avg nightly price in $)")
    plt.xticks(rotation=20, ha="right")
    plt.tight_layout()

    out = config.OUTPUTS_DIR / "fig6_price_size_matrix.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    log.info(f"Saved {out}")
    return str(out)


def main():
    apply_style()
    qe = QueryEngine()

    figs = [
        ("Borough comparison",        fig_borough_comparison),
        ("Price distribution",        fig_price_distribution),
        ("Neighborhood heatmap",      fig_neighborhood_heatmap),
        ("Superhost premium",         fig_superhost_premium),
        ("Availability vs price",     fig_availability_price),
        ("Price × size matrix",       fig_price_size_matrix),
    ]

    generated = []
    for label, fn in figs:
        log.info(f"Generating: {label}...")
        try:
            path = fn(qe)
            generated.append(path)
        except Exception as e:
            log.warning(f"  Skipped {label}: {e}")

    print(f"\nGenerated {len(generated)} figures in {config.OUTPUTS_DIR}/")
    for p in generated:
        print(f"  {p}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
