from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
import time
from pathlib import Path
from typing import Iterable, Iterator, List, Optional, Sequence, Tuple

CURRENT_FILE = Path(__file__).resolve()
SERVER_DIR = CURRENT_FILE.parent
PROJECT_ROOT = SERVER_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(SERVER_DIR) not in sys.path:
    sys.path.insert(0, str(SERVER_DIR))

try:
    from db import DatabaseManager
except ImportError:
    from server.db import DatabaseManager


BASICS_COLUMNS = [
    "tconst",
    "titleType",
    "primaryTitle",
    "originalTitle",
    "isAdult",
    "startYear",
    "endYear",
    "runtimeMinutes",
    "genres",
]

RATINGS_COLUMNS = [
    "tconst",
    "averageRating",
    "numVotes",
]


def none_if_missing(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = value.strip()
    if value == "" or value == r"\N":
        return None
    return value


def to_int(value: Optional[str]) -> Optional[int]:
    value = none_if_missing(value)
    return int(value) if value is not None else None


def to_float(value: Optional[str]) -> Optional[float]:
    value = none_if_missing(value)
    return float(value) if value is not None else None


def parse_title_types(raw: Optional[str]) -> Optional[List[str]]:
    if raw is None:
        return ["movie", "tvSeries"]
    cleaned = [part.strip() for part in raw.split(",") if part.strip()]
    return cleaned or None


def batched(rows: Iterable[Tuple], batch_size: int) -> Iterator[List[Tuple]]:
    batch: List[Tuple] = []
    for row in rows:
        batch.append(row)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def basics_rows(
    path: Path,
    allowed_title_types: Optional[Sequence[str]],
) -> Iterator[Tuple]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for raw in reader:
            title_type = none_if_missing(raw.get("titleType"))
            if allowed_title_types is not None and title_type not in allowed_title_types:
                continue

            yield (
                none_if_missing(raw.get("tconst")),
                title_type,
                none_if_missing(raw.get("primaryTitle")),
                none_if_missing(raw.get("originalTitle")),
                to_int(raw.get("isAdult")),
                to_int(raw.get("startYear")),
                to_int(raw.get("endYear")),
                to_int(raw.get("runtimeMinutes")),
                none_if_missing(raw.get("genres")),
            )


def ratings_rows(path: Path) -> Iterator[Tuple]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for raw in reader:
            yield (
                none_if_missing(raw.get("tconst")),
                to_float(raw.get("averageRating")),
                to_int(raw.get("numVotes")),
            )


def apply_fast_import_pragmas(conn: sqlite3.Connection) -> None:
    # Good for rebuilding a local DB from source TSVs.
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA temp_store = MEMORY")
    conn.execute("PRAGMA cache_size = -200000")  # ~200MB cache
    conn.execute("PRAGMA foreign_keys = ON")


def create_staging_tables(conn: sqlite3.Connection, *, drop_existing: bool) -> None:
    if drop_existing:
        conn.execute("DROP TABLE IF EXISTS title_basics_import")
        conn.execute("DROP TABLE IF EXISTS title_ratings_import")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS title_basics_import (
            tconst TEXT PRIMARY KEY,
            titleType TEXT,
            primaryTitle TEXT,
            originalTitle TEXT,
            isAdult INTEGER,
            startYear INTEGER,
            endYear INTEGER,
            runtimeMinutes INTEGER,
            genres TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS title_ratings_import (
            tconst TEXT PRIMARY KEY,
            averageRating REAL,
            numVotes INTEGER
        )
        """
    )


def create_final_table(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    drop_existing: bool,
) -> None:
    if drop_existing:
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")

    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            tconst TEXT PRIMARY KEY,
            titleType TEXT,
            primaryTitle TEXT,
            originalTitle TEXT,
            isAdult INTEGER,
            startYear INTEGER,
            endYear INTEGER,
            runtimeMinutes INTEGER,
            genres TEXT,
            averageRating REAL,
            numVotes INTEGER
        )
        """
    )
    conn.execute(f"DELETE FROM {table_name}")
    conn.execute(
        f"""
        INSERT INTO {table_name} (
            tconst,
            titleType,
            primaryTitle,
            originalTitle,
            isAdult,
            startYear,
            endYear,
            runtimeMinutes,
            genres,
            averageRating,
            numVotes
        )
        SELECT
            b.tconst,
            b.titleType,
            b.primaryTitle,
            b.originalTitle,
            b.isAdult,
            b.startYear,
            b.endYear,
            b.runtimeMinutes,
            b.genres,
            r.averageRating,
            r.numVotes
        FROM title_basics_import AS b
        LEFT JOIN title_ratings_import AS r
            ON r.tconst = b.tconst
        """
    )


def create_indexes(conn: sqlite3.Connection, table_name: str) -> None:
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_primaryTitle ON {table_name}(primaryTitle)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_titleType ON {table_name}(titleType)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_startYear ON {table_name}(startYear)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_averageRating ON {table_name}(averageRating)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_numVotes ON {table_name}(numVotes)"
    )


def load_into_table(
    conn: sqlite3.Connection,
    *,
    insert_sql: str,
    row_iter: Iterable[Tuple],
    batch_size: int,
    label: str,
) -> int:
    total = 0
    started = time.time()

    for batch in batched(row_iter, batch_size=batch_size):
        conn.executemany(insert_sql, batch)
        conn.commit()
        total += len(batch)
        elapsed = time.time() - started
        rate = total / elapsed if elapsed > 0 else 0.0
        print(f"[{label}] inserted {total:,} rows ({rate:,.0f} rows/sec)")

    return total


def import_tsvs(
    *,
    db_path: Path,
    basics_tsv: Path,
    ratings_tsv: Path,
    table_name: str,
    dataset_name: str,
    batch_size: int,
    title_types: Optional[Sequence[str]],
    drop_existing: bool,
) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        apply_fast_import_pragmas(conn)
        create_staging_tables(conn, drop_existing=drop_existing)

        if drop_existing:
            conn.execute("DELETE FROM title_basics_import")
            conn.execute("DELETE FROM title_ratings_import")
            conn.commit()

        basics_count = load_into_table(
            conn,
            insert_sql=(
                "INSERT OR REPLACE INTO title_basics_import "
                "(tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
            ),
            row_iter=basics_rows(basics_tsv, title_types),
            batch_size=batch_size,
            label="title.basics",
        )

        ratings_count = load_into_table(
            conn,
            insert_sql=(
                "INSERT OR REPLACE INTO title_ratings_import "
                "(tconst, averageRating, numVotes) VALUES (?, ?, ?)"
            ),
            row_iter=ratings_rows(ratings_tsv),
            batch_size=batch_size,
            label="title.ratings",
        )

        print("[merge] building final table...")
        create_final_table(conn, table_name=table_name, drop_existing=drop_existing)
        create_indexes(conn, table_name)
        conn.commit()

        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"[merge] final table '{table_name}' has {row_count:,} rows")

    finally:
        conn.close()

    manager = DatabaseManager(str(db_path))
    manager.register_dataset(
        dataset_name=dataset_name,
        table_name=table_name,
        search_column="primaryTitle",
        primary_key_column="tconst",
    )

    print()
    print("Import complete.")
    print(f"- db file      : {db_path}")
    print(f"- dataset name : {dataset_name}")
    print(f"- table name   : {table_name}")
    print(f"- basics rows  : {basics_count:,}")
    print(f"- ratings rows : {ratings_count:,}")
    if title_types is None:
        print("- title types  : all")
    else:
        print(f"- title types  : {', '.join(title_types)}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Stream IMDb TSV files into SQLite without loading everything into memory."
    )
    parser.add_argument(
        "--basics-tsv",
        required=True,
        help="Path to title.basics.tsv",
    )
    parser.add_argument(
        "--ratings-tsv",
        required=True,
        help="Path to title.ratings.tsv",
    )
    parser.add_argument(
        "--db-path",
        default=str((SERVER_DIR / "db.db").resolve()),
        help="SQLite output path (default: server/db.db)",
    )
    parser.add_argument(
        "--table-name",
        default="movies",
        help="Final SQLite table name (default: movies)",
    )
    parser.add_argument(
        "--dataset-name",
        default="movies",
        help="Dataset registry name (default: movies)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10000,
        help="Rows per batch insert (default: 10000)",
    )
    parser.add_argument(
        "--title-types",
        default="movie,tvSeries",
        help=(
            "Comma-separated titleType filter. "
            "Default: movie,tvSeries. Use --title-types '' to import all types."
        ),
    )
    parser.add_argument(
        "--drop-existing",
        action="store_true",
        help="Drop/rebuild the staging tables and final table before importing.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    title_types = parse_title_types(args.title_types)

    import_tsvs(
        db_path=Path(args.db_path).resolve(),
        basics_tsv=Path(args.basics_tsv).resolve(),
        ratings_tsv=Path(args.ratings_tsv).resolve(),
        table_name=args.table_name,
        dataset_name=args.dataset_name,
        batch_size=max(1, int(args.batch_size)),
        title_types=title_types,
        drop_existing=bool(args.drop_existing),
    )


if __name__ == "__main__":
    main()
