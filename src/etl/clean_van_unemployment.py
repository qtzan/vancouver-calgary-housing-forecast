from pathlib import Path
import re
import pandas as pd

## Need to possible fix how years are broken down 

BASE_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = BASE_DIR / "data_raw"
OUT_DIR = BASE_DIR / "data_clean"
OUT_DIR.mkdir(parents=True, exist_ok=True)

FILES = [
    RAW_DIR / "unemployment_raw2024.csv",
    RAW_DIR / "unemployment_raw2025.csv",
]

OUTPUT_PATH = OUT_DIR / "unemployment_van_cgy_monthly.csv"

PAT_YY_MON = re.compile(r"^\d{2}-[A-Za-z]{3}$")   # 10-Jan
PAT_MON_YY = re.compile(r"^[A-Za-z]{3}-\d{2}$")   # Jan-25


def read_raw_grid(path: Path) -> pd.DataFrame:
   
    for enc in ("utf-8-sig", "cp1252", "latin1"):
        try:
            return pd.read_csv(path, header=None, encoding=enc)
        except UnicodeDecodeError:
            continue
   
    return pd.read_csv(path, header=None, encoding="cp1252")


def parse_month_label(s: str) -> pd.Timestamp:
    s = str(s).strip()
    if PAT_YY_MON.match(s):
        return pd.to_datetime(s, format="%y-%b", errors="coerce")
    if PAT_MON_YY.match(s):
        return pd.to_datetime(s, format="%b-%y", errors="coerce")
    return pd.NaT


def extract_from_file(path: Path) -> pd.DataFrame:
    raw = read_raw_grid(path)


    header_row = None
    month_cols = None
    month_labels = None

    for r in range(len(raw)):
        row_vals = raw.iloc[r].tolist()
        cols = []
        labels = []
        for c, v in enumerate(row_vals):
            lab = str(v).strip()
            if PAT_YY_MON.match(lab) or PAT_MON_YY.match(lab):
                cols.append(c)
                labels.append(lab)

        
        if len(cols) >= 6:
            header_row, month_cols, month_labels = r, cols, labels
            break

    if header_row is None:
        raise ValueError(f"Could not find month header row in {path.name}")

 
    def find_row_contains(keyword: str) -> int:
        for r in range(header_row + 1, len(raw)):
            row_text = " ".join(raw.iloc[r].astype(str).fillna("").tolist())
            if keyword in row_text:
                return r
        return -1

    r_cgy = find_row_contains("Calgary")
    r_van = find_row_contains("Vancouver")

    if r_cgy == -1 or r_van == -1:
        raise ValueError(f"Could not find both Calgary and Vancouver rows in {path.name}")


    def build_city_df(row_idx: int, city_name: str) -> pd.DataFrame:
        vals = raw.iloc[row_idx, month_cols].tolist()
        out = pd.DataFrame({"month": month_labels, "unemployment_rate": vals})
        out["date"] = out["month"].apply(parse_month_label)
        out["unemployment_rate"] = pd.to_numeric(
            pd.Series(out["unemployment_rate"]).astype(str).str.replace(",", "", regex=False),
            errors="coerce",
        )
        out = out.dropna(subset=["date", "unemployment_rate"])
        out["city"] = city_name
        return out[["date", "city", "unemployment_rate"]]

    cgy = build_city_df(r_cgy, "Calgary")
    van = build_city_df(r_van, "Vancouver")

    return pd.concat([cgy, van], ignore_index=True)



all_parts = [extract_from_file(p) for p in FILES]
df = pd.concat(all_parts, ignore_index=True)


df = df.drop_duplicates(subset=["date", "city"], keep="last")


df = df[(df["date"] >= "2010-01-01") & (df["date"] <= "2026-01-01")]

df = df.sort_values(["city", "date"]).reset_index(drop=True)
df.to_csv(OUTPUT_PATH, index=False)

print("Saved:", OUTPUT_PATH)
print("Rows:", len(df))
print("Date range:", df["date"].min(), "to", df["date"].max())