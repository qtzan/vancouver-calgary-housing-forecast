
from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[2]
INPUT_PATH = BASE_DIR / "data_raw" / "Interest_Rate.csv"
OUT_DIR = BASE_DIR / "data_clean"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_PATH = OUT_DIR / "interest_rate_monthly.csv"

with open(INPUT_PATH, "r", encoding="utf-8-sig") as f:
    lines = f.readlines()


start_idx = next(
    i for i, line in enumerate(lines)
    if line.strip().split(",")[0].strip().strip('"').lower() == "date"
)

df = pd.read_csv(INPUT_PATH, skiprows=start_idx)


df = df.rename(columns={"V122514": "overnight_rate"})
df["date"] = pd.to_datetime(df["date"], errors="coerce")
df["overnight_rate"] = pd.to_numeric(df["overnight_rate"], errors="coerce")
df = df.dropna(subset=["date", "overnight_rate"])


df = df[(df["date"] >= "2010-01-01") & (df["date"] <= "2026-01-01")]
df = df.sort_values("date").reset_index(drop=True)

df.to_csv(OUTPUT_PATH, index=False)
print("Saved:", OUTPUT_PATH)