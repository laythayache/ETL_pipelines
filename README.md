# ETL_pipelines
to study and showcase the efficiency and robustness of etl pipelines on  low level data



# CSV → SQLite ETL Pipeline Manual

## 1. Purpose

This repository contains a minimal Extract‑Transform‑Load (ETL) pipeline implemented in pure Python. It accepts one or more CSV files, applies configurable cleaning rules with pandas, and stores the processed data in a local SQLite database for analysis.

## 2. Prerequisites

| Requirement      | Version / Notes                          |
| ---------------- | ---------------------------------------- |
| Python           | 3.9 or newer                             |
| pip              | Latest recommended                       |
| Operating System | Windows, macOS, or Linux (tested on all) |

## 3. Installation

1. Clone or copy the repository into your working directory.
2. Open a terminal in the project root (`etl_csv`).
3. Create a virtual environment and install dependencies:

```bash
python -m venv .venv
# Linux/macOS
source .venv/bin/activate
# Windows PowerShell
.venv\Scripts\Activate.ps1

pip install -r requirements.txt
```

## 4. Directory Layout

```
etl_csv/
├── data/            # Place raw CSV files here
├── sqlite/          # SQLite database file will appear here
├── clean_rules.py   # Reusable cleaning helpers
├── etl.py           # Main ETL orchestrator
├── requirements.txt # Dependency list
└── README.md        # This manual
```

## 5. Configuration

Open **`etl.py`** and review the constants near the top:

| Constant     | Description                                                     |
| ------------ | --------------------------------------------------------------- |
| `DATA_DIR`   | Folder containing the CSV files (default: `data/`)              |
| `DB_FILE`    | SQLite database path (default: `sqlite/warehouse.db`)           |
| `TABLE`      | Destination table name (default: `sales_raw`)                   |
| `DTYPE_MAP`  | Column‑to‑dtype overrides for casting (dictionary)              |
| `CHUNK_SIZE` | Number of rows per streaming chunk when reading large CSV files |

Adjust these values as required for your dataset.

## 6. Running the Pipeline

1. **Copy CSV files** into the `data/` directory.
2. **Execute** the script:

```bash
python etl.py
```

3. Monitor console output for progress logs. A successful run ends with:

```
… Loading <file>.csv
… <file>.csv finished
… Pipeline complete
```

## 7. Verifying the Load

### Using the SQLite CLI

```bash
sqlite3 sqlite/warehouse.db
sqlite> .tables
sqlite> SELECT COUNT(*) FROM sales_raw;
sqlite> .quit
```

### Using pandas

```python
import pandas as pd, sqlalchemy as sa
engine = sa.create_engine("sqlite:///sqlite/warehouse.db")
frame = pd.read_sql("SELECT * FROM sales_raw LIMIT 5", engine)
print(frame)
```

## 8. Updating Cleaning Logic

1. Add or modify helper functions in **`clean_rules.py`**.
2. Import and call them from `clean_df()` inside **`etl.py`**.
3. Re‑run the pipeline to apply the new rules.

## 9. Common Issues and Fixes

| Symptom                            | Likely Cause                            | Remedy                                        |
| ---------------------------------- | --------------------------------------- | --------------------------------------------- |
| `DATA_DIR does not exist` error    | Running script from the wrong directory | `cd etl_csv` before executing `python etl.py` |
| Duplicate rows after load          | Deduplication subset not configured     | Edit `clean_df()` to specify `subset=[…]`     |
| Type casting failed / mixed dtypes | `DTYPE_MAP` missing a column            | Add or correct the mapping in `etl.py`        |

## 10. Extending the Project

| Goal                          | Approach                                                           |
| ----------------------------- | ------------------------------------------------------------------ |
| Migrate to Postgres/MySQL     | Change the SQLAlchemy URI: `sqlite:///` → `postgresql+psycopg://…` |
| Validate data quality         | Integrate `pandera` or `Great Expectations` after cleaning         |
| Automate daily loads          | Create a cron job (Linux/macOS) or Task Scheduler entry (Windows)  |
| Scale beyond a single machine | Offload the **Load** phase to a cloud data warehouse               |

## 11. License

This project is released under the MIT License. Refer to the `LICENSE` file for the full text.
