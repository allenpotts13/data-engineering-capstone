import os
from datetime import datetime

import duckdb

from src.utils.logger import setup_logger

DUCKDB_PATH = os.path.join(os.getcwd(), "data", "duckdb", "motorcycle_capstone.duckdb")
SQL_DIR = os.path.join(os.getcwd(), "data", "sql")

logger = setup_logger(__name__, log_file="src/logs/duckdb_transformation.log")


def run_sql_scripts(db_path, sql_dir):
    con = duckdb.connect(db_path)
    success = True
    for filename in os.listdir(sql_dir):
        if filename.endswith(".sql"):
            sql_path = os.path.join(sql_dir, filename)
            with open(sql_path, "r") as sql_file:
                sql = sql_file.read()
            logger.info(f"Running {filename}...")
            try:
                con.execute(sql)
                logger.info(f"Successfully ran {filename}.")
            except Exception as e:
                logger.error(f"Error running {filename}: {e}")
                success = False
    con.close()
    return success


def main():
    con = duckdb.connect(DUCKDB_PATH)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS transformation_config (
            id INTEGER PRIMARY KEY,
            last_run TIMESTAMP
        );
    """
    )
    result = con.execute(
        "SELECT last_run FROM transformation_config WHERE id = 1;"
    ).fetchone()
    last_run = result[0] if result and result[0] else None
    logger.info(f"Last transformation run: {last_run}")

    if last_run:
        new_files = con.execute(
            "SELECT COUNT(*) FROM ingest_config WHERE ingested_at > ?", [last_run]
        ).fetchone()[0]
    else:
        new_files = con.execute("SELECT COUNT(*) FROM ingest_config;").fetchone()[0]

    if new_files > 0:
        logger.info(
            f"{new_files} new files ingested since last transformation. Running transformations."
        )
        success = run_sql_scripts(DUCKDB_PATH, SQL_DIR)
        if success:
            now = datetime.now()
            con.execute(
                """
                INSERT INTO transformation_config (id, last_run)
                VALUES (1, ?)
                ON CONFLICT (id) DO UPDATE SET last_run=excluded.last_run;
            """,
                [now],
            )
            logger.info(f"Transformation run completed. Updated last_run to {now}.")
        else:
            logger.error("Transformation run failed. last_run not updated.")
        con.close()
        return success
    else:
        logger.info(
            "No new files ingested since last transformation. Skipping transformations."
        )
        con.close()
        return True


if __name__ == "__main__":
    main()
