# src/etl/clean_housing_supply_van.py
from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[2]
INPUT_PATH = BASE_DIR / "data_raw" / "Housing_Supply_Van.csv"
OUTPUT_PATH = BASE_DIR / "data_clean" / "housing_starts_vancouver_monthly.csv"


df = pd.read_csv(INPUT_PATH, skiprows=2, encoding="cp1252")


df = df.dropna(axis=1, how="all")


df = df.iloc[:, [0, -1]].copy()
df.columns = ["date", "housing_starts_total"]


df["date"] = pd.to_datetime(df["date"], errors="coerce")


df["housing_starts_total"] = df["housing_starts_total"].astype(str).str.replace(",", "", regex=False)
df["housing_starts_total"] = pd.to_numeric(df["housing_starts_total"], errors="coerce")


df = df.dropna(subset=["date", "housing_starts_total"])


df = df[(df["date"] >= "2010-01-01") & (df["date"] <= "2026-01-01")]


df["city"] = "Vancouver"


df = df[["date", "city", "housing_starts_total"]].sort_values("date").reset_index(drop=True)

df.to_csv(OUTPUT_PATH, index=False)
print("Saved:", OUTPUT_PATH)