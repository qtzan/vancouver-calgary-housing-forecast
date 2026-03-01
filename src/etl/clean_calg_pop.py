from pathlib import Path
import pandas as pd

## NEED TO FIX LAST EMPTY ROW 
BASE_DIR = Path(__file__).resolve().parents[2]
INPUT_PATH = BASE_DIR / "data_raw" / "Calgary_Pop_Raw.csv"
OUT_DIR = BASE_DIR / "data_clean"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_PATH = OUT_DIR / "Calgary_Pop_Clean.csv"

df = pd.read_csv(INPUT_PATH)

df = df.rename(columns={"Unnamed: 0": "year"})

df["Date"] = pd.to_datetime(df["year"].astype(str) + "-01-01", errors="coerce")
df = df[["Date", "Population"]].dropna(subset=["Date"]).sort_values("Date")
df = df.set_index("Date")


monthly_index = pd.date_range(start="2010-01-01", end="2026-01-01", freq="MS")


df_monthly = df.reindex(monthly_index).ffill()



df_monthly = df_monthly.reset_index().rename(columns={"index": "Date"})
df_monthly["City"] = "Calgary"
df_monthly = df_monthly[["Date", "City", "Population"]]



df_monthly.to_csv(OUTPUT_PATH, index=False)
print("Saved:", OUTPUT_PATH)