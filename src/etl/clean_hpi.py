# src/etl/clean_hpi.py
from __future__ import annotations

from pathlib import Path
import pandas as pd


def main() -> None:
    # --- Paths (works no matter where you run the script from) ---
    BASE_DIR = Path(__file__).resolve().parents[2]  # project root
    INPUT_PATH = BASE_DIR / "data_raw" / "House_Price_Index.csv"
    OUT_DIR = BASE_DIR / "data_clean"
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    OUT_LONG = OUT_DIR / "hpi_van_cgy_long.csv"
    OUT_WIDE = OUT_DIR / "hpi_van_cgy_wide.csv"

    # --- Load ---
    raw = pd.read_csv(INPUT_PATH)

    # Row 0 contains subheaders like: Index, SA Index, Smoothed..., Sales Pair Count
    subheaders = raw.iloc[0]

    # Build new column names by carrying forward the last "base" city column
    new_cols = []
    base = None

    for col in raw.columns:
        if col == "Transaction Date":
            new_cols.append("date")
            base = None
            continue

        if not str(col).startswith("Unnamed"):
            base = col  # e.g., bc_vancouver, ab_calgary
            sub = subheaders[col]
        else:
            sub = subheaders[col]

        sub_clean = str(sub).strip().lower().replace(" ", "_")
        new_cols.append(f"{base}_{sub_clean}")

    df = raw.copy()
    df.columns = new_cols
    df = df.iloc[1:].copy()  # drop the subheader row

    # --- Parse dates (e.g., "Jun-1990") ---
    df["date"] = pd.to_datetime(df["date"], format="%b-%Y", errors="coerce")
    df = df.dropna(subset=["date"])

    # --- Keep only Vancouver + Calgary + required series ---
    # If you want different series, change these 4 columns:
    keep_cols = [
        "date",
        "bc_vancouver_index",
        "bc_vancouver_sa_index",
        "ab_calgary_index",
        "ab_calgary_sa_index",
    ]

    # Make sure the columns exist (fails fast with a helpful message)
    missing = [c for c in keep_cols if c not in df.columns]
    if missing:
        raise KeyError(
            f"Missing expected columns: {missing}\n"
            f"Available columns (sample): {list(df.columns)[:30]}"
        )

    df = df[keep_cols].copy()

    # --- Convert numeric columns ---
    for c in keep_cols[1:]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # --- Filter to everything from year 2000 onward ---
    df = df[df["date"] >= "2000-01-01"].copy()

    # --- Sort ---
    df = df.sort_values("date").reset_index(drop=True)

    # --- Save WIDE (optional but useful) ---
    df.to_csv(OUT_WIDE, index=False)

    # --- Create LONG (best for SQL + Tableau) ---
    long_df = df.melt(
        id_vars="date",
        value_vars=["bc_vancouver_sa_index", "ab_calgary_sa_index"],
        var_name="series",
        value_name="hpi_sa",
    )

    long_df["city"] = long_df["series"].map(
        {
            "bc_vancouver_sa_index": "Vancouver",
            "ab_calgary_sa_index": "Calgary",
        }
    )
    long_df = (
        long_df.drop(columns=["series"])
        .dropna(subset=["hpi_sa"])
        .sort_values(["city", "date"])
        .reset_index(drop=True)
    )

    long_df.to_csv(OUT_LONG, index=False)

    print("Saved:")
    print(f"- {OUT_WIDE}")
    print(f"- {OUT_LONG}")


if __name__ == "__main__":
    main()