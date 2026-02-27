# src/etl/clean_housing_supply_van.py
from pathlib import Path
import re
import pandas as pd
from io import StringIO

BASE_DIR = Path(__file__).resolve().parents[2]
INPUT_PATH = BASE_DIR / "data_raw" / "Housing_Supply_Van.csv"

OUT_DIR = BASE_DIR / "data_clean"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_PATH = OUT_DIR / "housing_starts_vancouver_monthly.csv"

# --- read as text (handle encoding) ---
try:
    text = INPUT_PATH.read_text(encoding="utf-8-sig")
except UnicodeDecodeError:
    text = INPUT_PATH.read_text(encoding="cp1252")

lines = [l for l in text.splitlines() if l.strip()]

# --- detect delimiter using multiple lines (not just the first) ---
sample = "\n".join(lines[:50])
delim = "\t" if sample.count("\t") > sample.count(",") else ","

def looks_numeric(x: str) -> bool:
    x = x.strip().strip('"').replace(",", "")
    return bool(re.match(r"^-?\d+(\.\d+)?$", x))

start_idx = None
date_col = None

# Find the first row that has:
# - a parseable date in one of the first 3 columns
# - a numeric value in the LAST column (the "All" total)
for i, line in enumerate(lines):
    cells = [c.strip().strip('"') for c in re.split(r"[,\t]", line)]
    if len(cells) < 2:
        continue

    last = cells[-1]
    if not looks_numeric(last):
        continue

    for c in range(min(3, len(cells))):
        dt = pd.to_datetime(cells[c], errors="coerce")
        if pd.notna(dt) and 1980 <= dt.year <= 2030:
            start_idx = i
            date_col = c
            break

    if start_idx is not None:
        break

if start_idx is None:
    raise ValueError("Could not find the first data row (date + numeric total).")

data_text = "\n".join(lines[start_idx:])
data = pd.read_csv(StringIO(data_text), header=None, sep=delim, engine="python")

df = pd.DataFrame({
    "date": pd.to_datetime(data.iloc[:, date_col], errors="coerce"),
    "housing_starts_total": pd.to_numeric(data.iloc[:, -1], errors="coerce"),
})

df = df.dropna(subset=["date", "housing_starts_total"])

# Force monthly month-start dates
df["date"] = df["date"].dt.to_period("M").dt.to_timestamp()

# Filter to your window (inclusive)
df = df[(df["date"] >= "2010-01-01") & (df["date"] <= "2026-01-01")]

df["city"] = "Vancouver"
df = df[["date", "city", "housing_starts_total"]].sort_values(["city", "date"]).reset_index(drop=True)

df.to_csv(OUTPUT_PATH, index=False)

print("Saved:", OUTPUT_PATH)
print("Rows:", len(df))
print("Date range:", df["date"].min(), "to", df["date"].max())