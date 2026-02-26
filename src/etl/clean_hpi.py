# src/etl/clean_hpi.py
from __future__ import annotations

from pathlib import Path
import pandas as pd


def main() -> None:
    
    BASE_DIR = Path(__file__).resolve().parents[2]  
    INPUT_PATH = BASE_DIR / "data_raw" / "House_Price_Index.csv"
    OUT_DIR = BASE_DIR / "data_clean"
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    OUT_LONG = OUT_DIR / "hpi_van_cgy_long.csv"
    OUT_WIDE = OUT_DIR / "hpi_van_cgy_wide.csv"

  
    raw = pd.read_csv(INPUT_PATH)

   
    subheaders = raw.iloc[0]

    new_cols = []
    base = None

    for col in raw.columns:
        if col == "Transaction Date":
            new_cols.append("date")
            base = None
            continue

        if not str(col).startswith("Unnamed"):
            base = col  
            sub = subheaders[col]
        else:
            sub = subheaders[col]

        sub_clean = str(sub).strip().lower().replace(" ", "_")
        new_cols.append(f"{base}_{sub_clean}")

    df = raw.copy()
    df.columns = new_cols
    df = df.iloc[1:].copy()  

    
    df["date"] = pd.to_datetime(df["date"], format="%b-%Y", errors="coerce")
    df = df.dropna(subset=["date"])

   
    keep_cols = [
        "date",
        "bc_vancouver_index",
        "bc_vancouver_sa_index",
        "ab_calgary_index",
        "ab_calgary_sa_index",
    ]

  
    missing = [c for c in keep_cols if c not in df.columns]
    if missing:
        raise KeyError(
            f"Missing expected columns: {missing}\n"
            f"Available columns (sample): {list(df.columns)[:30]}"
        )

    df = df[keep_cols].copy()

  
    for c in keep_cols[1:]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

   
    df = df[df["date"] >= "2010-01-01"].copy()

 
    df = df.sort_values("date").reset_index(drop=True)

   
    df.to_csv(OUT_WIDE, index=False)

   
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