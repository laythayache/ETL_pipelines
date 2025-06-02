"""
Reusable cleaning helpers for the ETL pipeline.
"""

import pandas as pd


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Strip, lower-case, and snake_case all column names.
    """
    df = df.copy()
    df.columns = (
        df.columns
          .str.strip()
          .str.lower()
          .str.replace(r"[^\w]+", "_", regex=True)
    )
    return df


def enforce_types(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    """
    Cast columns to dtypes specified in *schema* if present.
    `schema` example: {'order_date': 'datetime64[ns]', 'price': 'float64'}
    """
    df = df.copy()
    for col, dtype in schema.items():
        if col in df.columns:
            if dtype.startswith("datetime"):
                df[col] = pd.to_datetime(df[col], errors="coerce")
            else:
                df[col] = df[col].astype(dtype, errors="ignore")
    return df


def drop_dupes_na(df: pd.DataFrame, subset=None) -> pd.DataFrame:
    """
    Remove duplicate rows (optionally on *subset*) and drop rows that are all-NaN.
    """
    df = df.copy()
    df = df.drop_duplicates(subset=subset)
    return df.dropna(how="all")
