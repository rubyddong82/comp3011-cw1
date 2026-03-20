#!/usr/bin/env python3
import argparse
import csv
import os
import sqlite3
import sys
import time
from typing import List, Sequence, Tuple

def raise_csv_field_limit():
    limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(limit)
            return
        except OverflowError:
            limit //= 10

raise_csv_field_limit()

IMDB_FILES = {
    "title.basics.tsv": {
        "table": "title_basics",
        "columns": [
            ("tconst", "TEXT"),
            ("titleType", "TEXT"),
            ("primaryTitle", "TEXT"),
            ("originalTitle", "TEXT"),
            ("isAdult", "INTEGER"),
            ("startYear", "INTEGER"),
            ("endYear", "INTEGER"),
            ("runtimeMinutes", "INTEGER"),
            ("genres", "TEXT"),
        ],
        "unique_indexes": [
            ("ux_title_basics_tconst", ["tconst"]),
        ],
        "indexes": [
            ("idx_title_basics_titleType", ["titleType"]),
            ("idx_title_basics_startYear", ["startYear"]),
            ("idx_title_basics_primaryTitle", ["primaryTitle"]),
        ],
    },
    "title.ratings.tsv": {
        "table": "title_ratings",
        "columns": [
            ("tconst", "TEXT"),
            ("averageRating", "REAL"),
            ("numVotes", "INTEGER"),
        ],
        "unique_indexes": [
            ("ux_title_ratings_tconst", ["tconst"]),
        ],
        "indexes": [
            ("idx_title_ratings_numVotes", ["numVotes"]),
            ("idx_title_ratings_averageRating", ["averageRating"]),
        ],
    },
    "name.basics.tsv": {
        "table": "name_basics",
        "columns": [
            ("nconst", "TEXT"),
            ("primaryName", "TEXT"),
            ("birthYear", "INTEGER"),
            ("deathYear", "INTEGER"),
            ("primaryProfession", "TEXT"),
            ("knownForTitles", "TEXT"),
        ],
        "unique_indexes": [
            ("ux_name_basics_nconst", ["nconst"]),
        ],
        "indexes": [
            ("idx_name_basics_primaryName", ["primaryName"]),
        ],
    },
    "title.principals.tsv": {
        "table": "title_principals",
        "columns": [
            ("tconst", "TEXT"),
            ("ordering", "INTEGER"),
            ("nconst", "TEXT"),
            ("category", "TEXT"),
            ("job", "TEXT"),
            ("characters", "TEXT"),
        ],
        "unique_indexes": [
            ("ux_title_principals_tconst_ordering", ["tconst", "ordering"]),
        ],
        "indexes": [
            ("idx_title_principals_tconst", ["tconst"]),
            ("idx_title_principals_nconst", ["nconst"]),
            ("idx_title_principals_category", ["category"]),
        ],
    },
    "title.akas.tsv": {
        "table": "title_akas",
        "columns": [
            ("titleId", "TEXT"),
            ("ordering", "INTEGER"),
            ("title", "TEXT"),
            ("region", "TEXT"),
            ("language", "TEXT"),
            ("types", "TEXT"),
            ("attributes", "TEXT"),
            ("isOriginalTitle", "INTEGER"),
        ],
        "unique_indexes": [
            ("ux_title_akas_titleId_ordering", ["titleId", "ordering"]),
        ],
        "indexes": [
            ("idx_title_akas_titleId", ["titleId"]),
            ("idx_title_akas_region", ["region"]),
            ("idx_title_akas_language", ["language"]),
        ],
    },
}

INTEGER_HINTS = {
    "isAdult", "startYear", "endYear", "runtimeMinutes", "numVotes",
    "birthYear", "deathYear", "ordering", "isOriginalTitle",
}
REAL_HINTS = {"averageRating"}


def log(msg: str) -> None:
    now = time.strftime("%H:%M:%S")
    print(f"[{now}] {msg}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fast import of IMDb TSV files into SQLite.")
    parser.add_argument("--tsv-dir", default=".", help="Directory containing TSV files.")
    parser.add_argument("--db-path", default="imdb.db", help="Output SQLite DB path.")
    parser.add_argument("--batch-size", type=int, default=50000, help="Rows per executemany batch during raw load.")
    parser.add_argument("--copy-batch-size", type=int, default=200000, help="Rows per chunk when copying raw -> final.")
    parser.add_argument("--drop-existing", action="store_true", help="Drop final tables before import.")
    parser.add_argument("--keep-raw", action="store_true", help="Keep __raw_* tables after success.")
    parser.add_argument("--only", nargs="*", choices=sorted(IMDB_FILES.keys()), help="Only import listed TSV files.")
    parser.add_argument("--progress-seconds", type=int, default=10, help="Report every N seconds.")
    parser.add_argument("--encoding", default="utf-8", help="Input TSV encoding.")
    parser.add_argument("--skip-line-count", action="store_true", help="Do not pre-scan files to count total rows.")
    parser.add_argument(
        "--safe-mode",
        action="store_true",
        help="Use safer but slower SQLite pragmas. Default is faster one-shot import mode.",
    )
    return parser.parse_args()


def sqlite_type_for_column(column_name: str) -> str:
    if column_name in INTEGER_HINTS:
        return "INTEGER"
    if column_name in REAL_HINTS:
        return "REAL"
    return "TEXT"


def convert_value(column_name: str, raw: str):
    if raw == r"\N" or raw == "":
        return None
    if column_name in INTEGER_HINTS:
        try:
            return int(raw)
        except ValueError:
            return None
    if column_name in REAL_HINTS:
        try:
            return float(raw)
        except ValueError:
            return None
    return raw


def ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)


def count_lines_fast(path: str, chunk_size: int = 1024 * 1024) -> int:
    total = 0
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            total += chunk.count(b"\n")
    return max(total - 1, 0)


def build_raw_table(conn: sqlite3.Connection, raw_table: str, header: Sequence[str]) -> None:
    cols = ", ".join(f'"{c}" {sqlite_type_for_column(c)}' for c in header)
    conn.execute(f'DROP TABLE IF EXISTS "{raw_table}"')
    conn.execute(f'CREATE TABLE "{raw_table}" ({cols})')


def build_final_table(conn: sqlite3.Connection, filename: str, header: Sequence[str], final_table: str) -> None:
    spec = IMDB_FILES[filename]
    declared = {name: decl for name, decl in spec["columns"]}
    column_defs: List[str] = []
    for col in header:
        decl = declared.get(col, sqlite_type_for_column(col))
        column_defs.append(f'"{col}" {decl}')
    conn.execute(f'DROP TABLE IF EXISTS "{final_table}"')
    conn.execute(f'CREATE TABLE "{final_table}" ({", ".join(column_defs)})')


def report_progress(filename: str, processed: int, total_rows: int | None, interval_start_rows: int, interval_start_time: float, started: float, stage: str) -> None:
    now = time.time()
    interval_elapsed = max(now - interval_start_time, 1e-9)
    total_elapsed = max(now - started, 1e-9)
    interval_rate = (processed - interval_start_rows) / interval_elapsed
    avg_rate = processed / total_elapsed
    if total_rows:
        percent = processed / total_rows * 100.0
        total_text = f"{processed:,}/{total_rows:,} rows ({percent:.2f}%)"
    else:
        total_text = f"{processed:,} rows"
    log(f"{filename} [{stage}]: {total_text} | interval {interval_rate:,.0f} rows/s | avg {avg_rate:,.0f} rows/s")


def insert_raw_batches(
    conn: sqlite3.Connection,
    filepath: str,
    filename: str,
    header: Sequence[str],
    raw_table: str,
    batch_size: int,
    progress_seconds: int,
    encoding: str,
    total_rows: int | None,
) -> int:
    placeholders = ", ".join("?" for _ in header)
    cols = ", ".join(f'"{c}"' for c in header)
    sql = f'INSERT INTO "{raw_table}" ({cols}) VALUES ({placeholders})'

    processed = 0
    batch: List[Tuple] = []
    started = time.time()
    last_report = started
    interval_start_time = started
    interval_start_rows = 0

    with open(filepath, "r", encoding=encoding, newline="") as f:
        reader = csv.reader(f, delimiter="\t")
        next(reader)

        conn.execute("BEGIN")
        for row in reader:
            if len(row) != len(header):
                if len(row) < len(header):
                    row = row + [""] * (len(header) - len(row))
                else:
                    row = row[: len(header)]

            batch.append(tuple(convert_value(col, val) for col, val in zip(header, row)))
            processed += 1

            if len(batch) >= batch_size:
                conn.executemany(sql, batch)
                batch.clear()

            now = time.time()
            if now - last_report >= progress_seconds:
                report_progress(filename, processed, total_rows, interval_start_rows, interval_start_time, started, "raw")
                last_report = now
                interval_start_time = now
                interval_start_rows = processed

        if batch:
            conn.executemany(sql, batch)

        conn.commit()

    elapsed = max(time.time() - started, 1e-9)
    log(f"Loaded raw {filename}: {processed:,} rows in {elapsed:.1f}s ({processed / elapsed:,.0f} rows/s)")
    return processed


def materialize_final_table_chunked(
    conn: sqlite3.Connection,
    filename: str,
    header: Sequence[str],
    raw_table: str,
    final_table: str,
    copy_batch_size: int,
    progress_seconds: int,
) -> int:
    cols = ", ".join(f'"{c}"' for c in header)
    total_rows = conn.execute(f'SELECT COUNT(*) FROM "{raw_table}"').fetchone()[0]
    max_rowid = conn.execute(f'SELECT COALESCE(MAX(rowid), 0) FROM "{raw_table}"').fetchone()[0]

    insert_sql = (
        f'INSERT INTO "{final_table}" ({cols}) '
        f'SELECT {cols} FROM "{raw_table}" '
        f'WHERE rowid > ? AND rowid <= ?'
    )

    copied = 0
    lo = 0
    started = time.time()
    last_report = started
    interval_start_time = started
    interval_start_rows = 0

    while lo < max_rowid:
        hi = lo + copy_batch_size
        conn.execute("BEGIN")
        conn.execute(insert_sql, (lo, hi))
        conn.commit()
        delta = conn.total_changes - copied  # not used reliably across prior ops
        lo = hi
        # count from final table because copy may skip impossible gaps if any
        copied = conn.execute(f'SELECT COUNT(*) FROM "{final_table}"').fetchone()[0]

        now = time.time()
        if now - last_report >= progress_seconds:
            report_progress(filename, copied, total_rows, interval_start_rows, interval_start_time, started, "final-copy")
            last_report = now
            interval_start_time = now
            interval_start_rows = copied

    elapsed = max(time.time() - started, 1e-9)
    log(f"Materialized final {final_table}: {copied:,} rows in {elapsed:.1f}s ({copied / elapsed:,.0f} rows/s)")
    return copied


def create_indexes(conn: sqlite3.Connection, filename: str) -> None:
    spec = IMDB_FILES[filename]
    table = spec["table"]
    for name, cols in spec.get("unique_indexes", []):
        col_sql = ", ".join(cols)
        conn.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS {name} ON "{table}"({col_sql})')
    for name, cols in spec.get("indexes", []):
        col_sql = ", ".join(cols)
        conn.execute(f'CREATE INDEX IF NOT EXISTS {name} ON "{table}"({col_sql})')
    conn.commit()


def import_one_file(
    conn: sqlite3.Connection,
    filepath: str,
    filename: str,
    batch_size: int,
    copy_batch_size: int,
    progress_seconds: int,
    encoding: str,
    drop_existing: bool,
    keep_raw: bool,
    skip_line_count: bool,
) -> None:
    total_rows = None if skip_line_count else count_lines_fast(filepath)
    if total_rows is None:
        log(f"Starting {filename}")
    else:
        log(f"Starting {filename} ({total_rows:,} data rows)")

    with open(filepath, "r", encoding=encoding, newline="") as f:
        reader = csv.reader(f, delimiter="\t")
        try:
            header = next(reader)
        except StopIteration:
            log(f"Skipping empty file: {filename}")
            return

    spec = IMDB_FILES[filename]
    final_table = spec["table"]
    raw_table = f"__raw_{final_table}"

    if drop_existing:
        conn.execute(f'DROP TABLE IF EXISTS "{final_table}"')
        conn.execute(f'DROP TABLE IF EXISTS "{raw_table}"')
        conn.commit()

    build_raw_table(conn, raw_table, header)
    conn.commit()

    insert_raw_batches(
        conn=conn,
        filepath=filepath,
        filename=filename,
        header=header,
        raw_table=raw_table,
        batch_size=batch_size,
        progress_seconds=progress_seconds,
        encoding=encoding,
        total_rows=total_rows,
    )

    log(f"Building final table {final_table} from raw data")
    build_final_table(conn, filename, header, final_table)
    conn.commit()

    materialize_final_table_chunked(
        conn=conn,
        filename=filename,
        header=header,
        raw_table=raw_table,
        final_table=final_table,
        copy_batch_size=copy_batch_size,
        progress_seconds=progress_seconds,
    )

    log(f"Creating indexes for {final_table}")
    create_indexes(conn, filename)

    if not keep_raw:
        conn.execute(f'DROP TABLE IF EXISTS "{raw_table}"')
        conn.commit()
    log(f"Finished {filename} -> {final_table}")


def configure_sqlite(conn: sqlite3.Connection, safe_mode: bool) -> None:
    conn.execute("PRAGMA foreign_keys = OFF")
    conn.execute("PRAGMA temp_store = MEMORY")
    conn.execute("PRAGMA cache_size = -200000")

    if safe_mode:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
    else:
        conn.execute("PRAGMA journal_mode = OFF")
        conn.execute("PRAGMA synchronous = OFF")


def main() -> int:
    args = parse_args()

    if args.batch_size <= 0:
        print("--batch-size must be > 0", file=sys.stderr)
        return 1
    if args.copy_batch_size <= 0:
        print("--copy-batch-size must be > 0", file=sys.stderr)
        return 1
    if args.progress_seconds <= 0:
        print("--progress-seconds must be > 0", file=sys.stderr)
        return 1

    files_to_import = args.only or list(IMDB_FILES.keys())
    missing = [
        os.path.join(args.tsv_dir, filename)
        for filename in files_to_import
        if not os.path.isfile(os.path.join(args.tsv_dir, filename))
    ]
    if missing:
        print("Missing required TSV files:", file=sys.stderr)
        for path in missing:
            print(f"  - {path}", file=sys.stderr)
        return 1

    ensure_parent_dir(args.db_path)
    conn = sqlite3.connect(args.db_path)
    try:
        configure_sqlite(conn, args.safe_mode)
        overall_start = time.time()
        for filename in files_to_import:
            import_one_file(
                conn=conn,
                filepath=os.path.join(args.tsv_dir, filename),
                filename=filename,
                batch_size=args.batch_size,
                copy_batch_size=args.copy_batch_size,
                progress_seconds=args.progress_seconds,
                encoding=args.encoding,
                drop_existing=args.drop_existing,
                keep_raw=args.keep_raw,
                skip_line_count=args.skip_line_count,
            )
        elapsed = max(time.time() - overall_start, 1e-9)
        log(f"All done in {elapsed:.1f}s. SQLite DB written to: {os.path.abspath(args.db_path)}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
