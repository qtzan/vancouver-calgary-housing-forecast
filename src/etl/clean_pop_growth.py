from pathlib import Path
import re
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = BASE_DIR / "data_raw"
OUT_DIR = BASE_DIR / "data_clean"
OUT_DIR.mkdir(parents=True, exist_ok=True)

FILES = [
    RAW_DIR / "Pop_Growth_Raw_2021.csv",
    RAW_DIR / "Pop_Growth_Raw_2025.csv",
]

OUTPUT_PATH = OUT_DIR / "migration_van_cgy_long.csv"

YEAR_PAT = re.compile(r"^\s*(\d{4})\s*/\s*(\d{4})\s*$")
INTER_TITLE = re.compile(r"net\s+interprovincial\s+migration", re.IGNORECASE)
INTRA_TITLE = re.compile(r"net\s+intraprovincial\s+migration", re.IGNORECASE)

def read_raw_grid(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "cp1252", "latin1"):
        try:
            return pd.read_csv(path, header=None, encoding=enc)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, header=None, encoding="cp1252")

def find_title_row(raw: pd.DataFrame) -> int:
    # row containing "Net interprovincial migration" / "Net intraprovincial migration"
    for r in range(len(raw)):
        row = " ".join(raw.iloc[r].astype(str).fillna("").tolist())
        if INTER_TITLE.search(row) or INTRA_TITLE.search(row):
            return r
    raise ValueError("Could not find the row containing the migration titles.")

def find_year_header_row(raw: pd.DataFrame) -> int:
    # row containing many "YYYY / YYYY"
    for r in range(len(raw)):
        hits = sum(1 for v in raw.iloc[r].tolist() if YEAR_PAT.match(str(v).strip()))
        if hits >= 3:
            return r
    raise ValueError("Could not find year header row with YYYY / YYYY columns.")

def find_geo_rows(raw: pd.DataFrame, year_header_row: int) -> tuple[int, int]:
    r_cgy = r_van = -1
    for r in range(year_header_row + 1, min(year_header_row + 50, len(raw))):
        row_text = " ".join(raw.iloc[r].astype(str).fillna("").tolist()).lower()
        if r_cgy == -1 and ("division no. 6" in row_text or "calgary" in row_text):
            r_cgy = r
        if r_van == -1 and ("greater vancouver" in row_text or "vancouver" in row_text):
            r_van = r
    if r_cgy == -1 or r_van == -1:
        raise ValueError("Could not find both Calgary and Vancouver rows.")
    return r_cgy, r_van

def find_title_col(raw: pd.DataFrame, title_row: int, title_regex: re.Pattern) -> int:
    for c, v in enumerate(raw.iloc[title_row].tolist()):
        if title_regex.search(str(v)):
            return c
    return -1

def year_cols_after(raw: pd.DataFrame, year_header_row: int, start_col: int) -> list[int]:
    cols = []
    for c in range(start_col, raw.shape[1]):
        if YEAR_PAT.match(str(raw.iloc[year_header_row, c]).strip()):
            cols.append(c)
        # stop once we've started collecting and then hit a long run of non-year cells
        elif cols and c - cols[-1] > 10:
            break
    return cols

def build_city(raw, year_header_row, cols, city_row, city_name, component):
    labels = [str(raw.iloc[year_header_row, c]).strip() for c in cols]
    vals = raw.iloc[city_row, cols].tolist()

    df = pd.DataFrame({"year_span": labels, "value": vals})

    yrs = df["year_span"].str.extract(r"(\d{4})\s*/\s*(\d{4})")
    df["end_year"] = pd.to_numeric(yrs[1], errors="coerce")
    df["date"] = pd.to_datetime(df["end_year"].astype("Int64").astype(str) + "-01-01", errors="coerce")

    df["value"] = pd.to_numeric(df["value"].astype(str).str.replace(",", "", regex=False), errors="coerce")
    df = df.dropna(subset=["date", "value"])

    df["city"] = city_name
    df["component"] = component
    return df[["date", "city", "component", "value"]]

def extract_one_file(path: Path) -> pd.DataFrame:
    raw = read_raw_grid(path)

    title_row = find_title_row(raw)
    year_header_row = find_year_header_row(raw)
    r_cgy, r_van = find_geo_rows(raw, year_header_row)

    inter_col = find_title_col(raw, title_row, INTER_TITLE)
    intra_col = find_title_col(raw, title_row, INTRA_TITLE)

    if inter_col == -1 or intra_col == -1:
        raise ValueError(f"Could not find both inter/intra title columns in {path.name}")

    inter_year_cols = year_cols_after(raw, year_header_row, inter_col)
    intra_year_cols = year_cols_after(raw, year_header_row, intra_col)

    if len(inter_year_cols) < 2 or len(intra_year_cols) < 2:
        raise ValueError(
            f"Year columns not found properly in {path.name}. "
            f"inter={len(inter_year_cols)}, intra={len(intra_year_cols)}"
        )

    out = pd.concat([
        build_city(raw, year_header_row, inter_year_cols, r_cgy, "Calgary", "net_interprovincial_migration"),
        build_city(raw, year_header_row, inter_year_cols, r_van, "Vancouver", "net_interprovincial_migration"),
        build_city(raw, year_header_row, intra_year_cols, r_cgy, "Calgary", "net_intraprovincial_migration"),
        build_city(raw, year_header_row, intra_year_cols, r_van, "Vancouver", "net_intraprovincial_migration"),
    ], ignore_index=True)

    return out

# ---- Run both files and combine ----
df = pd.concat([extract_one_file(p) for p in FILES], ignore_index=True)
df = df.drop_duplicates(subset=["date", "city", "component"], keep="last")
df = df.sort_values(["component", "city", "date"]).reset_index(drop=True)

df.to_csv(OUTPUT_PATH, index=False)

print("Saved:", OUTPUT_PATH)
print("Components:", df["component"].unique())
print("Min date:", df["date"].min(), "Max date:", df["date"].max())
print("Rows:", len(df))