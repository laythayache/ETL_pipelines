"""
Entry-point ETL script: CSV ➜ pandas clean ➜ SQLite.
Usage:  python etl.py
"""

import logging
import pathlib
import sys

import pandas as pd
from sqlalchemy import create_engine, text

from clean_rules import (
    standardize_columns,
    enforce_types,
    drop_dupes_na,
)

# ────────────────────── Configuration ──────────────────────
DATA_DIR   = pathlib.Path("data")                 # where CSVs live
DB_FILE    = pathlib.Path("sqlite/warehouse.db")  # SQLite destination
TABLE      = "sales_raw"                          # target table name
DTYPE_MAP  = {                                    # column type overrides
    "order_date": "datetime64[ns]",
    "price": "float64",
    "qty": "int64",
}
CHUNK_SIZE = 50_000                               # rows per chunk
LOG_LEVEL  = logging.INFO
# ────────────────────────────────────────────────────────────

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s ➜ %(message)s",
    datefmt="%H:%M:%S",
)

# Make sure destination folder exists before engine creation
DB_FILE.parent.mkdir(parents=True, exist_ok=True)
engine = create_engine(f"sqlite:///{DB_FILE}")


# ────────────────────── Helper functions ───────────────────
def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply all cleaning steps to a DataFrame chunk.
    """
    df = standardize_columns(df)
    df = enforce_types(df, DTYPE_MAP)
    # If the CSV has an order_id column, deduplicate on it
    subset = ["order_id"] if "order_id" in df.columns else None
    df = drop_dupes_na(df, subset=subset)
    return df


def csv_to_sqlite(csv_path: pathlib.Path) -> None:
    """
    Stream a single CSV into SQLite in CHUNK_SIZE pieces.
    """
    logging.info(f"⏳  Loading {csv_path.name}")
    for chunk in pd.read_csv(csv_path, chunksize=CHUNK_SIZE):
        cleaned = clean_df(chunk)
        cleaned.to_sql(TABLE, engine, if_exists="append", index=False)
    logging.info(f"✅  {csv_path.name} finished")


def add_indices() -> None:
    """
    Build useful indices after bulk load (keeps ingest faster).
    """
    with engine.begin() as conn:
        # Example: add an index on order_id if that column exists
        cols = [c[0] for c in conn.execute(text(f"PRAGMA table_info({TABLE})"))]
        if "order_id" in cols:
            conn.execute(
                text(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_order_id "
                     f"ON {TABLE}(order_id)")
            )


# ────────────────────────── Main ───────────────────────────
def main() -> None:
    if not DATA_DIR.exists():
        logging.error("DATA_DIR does not exist")
        sys.exit(1)

    csv_files = sorted(DATA_DIR.glob("*.csv"))
    if not csv_files:
        logging.warning("No CSV files in ./data — nothing to load.")
        return

    for csv in csv_files:
        csv_to_sqlite(csv)

    add_indices()
    logging.info("🏁  Pipeline complete")


if __name__ == "__main__":
    main()
