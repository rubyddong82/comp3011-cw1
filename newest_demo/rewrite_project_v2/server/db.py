from __future__ import annotations

import json
import re
import sqlite3
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any, Dict, Iterable, List, Optional, Tuple

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

SUPPORTED_FILTER_COLUMNS = [
    "titleType",
    "isAdult",
    "genres",
    "startYear",
    "runtimeMinutes",
    "averageRating",
    "numVotes",
]

NUMERIC_FILTER_COLUMNS = {"startYear", "runtimeMinutes", "averageRating", "numVotes"}
CATEGORICAL_FILTER_COLUMNS = {"titleType", "isAdult", "genres"}
PREFETCH_SAMPLE_RATIO = 0.10
QUERY_FETCH_CAP = 300


@dataclass(frozen=True)
class ColumnMeta:
    name: str
    declared_type: str
    normalized_type: str
    is_primary_key: bool
    is_search_column: bool
    is_filterable: bool
    is_editable: bool
    value_kind: str


class DBError(Exception):
    pass


class DatasetNotRegisteredError(DBError):
    pass


class InvalidIdentifierError(DBError):
    pass


class RowNotFoundError(DBError):
    pass


class ValidationError(DBError):
    pass


class DatabaseManager:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._query_config_cache: Dict[str, Dict[str, Any]] = {}
        self._filter_choice_cache: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._query_eval_cache: Dict[str, Dict[str, Any]] = {}
        self._ensure_internal_tables()

    @contextmanager
    def _connect(self) -> Iterable[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA temp_store = MEMORY")
        conn.execute("PRAGMA cache_size = -200000")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _ensure_internal_tables(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS datasets (
                    dataset_name TEXT PRIMARY KEY,
                    table_name TEXT NOT NULL UNIQUE,
                    search_column TEXT NOT NULL,
                    primary_key_column TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )

    # ----------------------------
    # Dataset registry
    # ----------------------------
    def register_dataset(
        self,
        dataset_name: str,
        table_name: str,
        search_column: Optional[str] = None,
        primary_key_column: Optional[str] = None,
    ) -> None:
        self._validate_identifier(dataset_name)
        self._validate_identifier(table_name)
        schema = self._introspect_table(table_name)
        column_names = {col["name"] for col in schema}

        resolved_search = search_column or "primaryTitle"
        resolved_pk = primary_key_column or "tconst"

        if resolved_search not in column_names:
            raise ValidationError(f"Search column '{resolved_search}' does not exist in table '{table_name}'.")
        if resolved_pk not in column_names:
            raise ValidationError(f"Primary key column '{resolved_pk}' does not exist in table '{table_name}'.")

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO datasets (
                    dataset_name, table_name, search_column, primary_key_column, created_at
                )
                VALUES (?, ?, ?, ?, datetime('now'))
                ON CONFLICT(dataset_name) DO UPDATE SET
                    table_name = excluded.table_name,
                    search_column = excluded.search_column,
                    primary_key_column = excluded.primary_key_column
                """,
                (dataset_name, table_name, resolved_search, resolved_pk),
            )
        self._invalidate_dataset_caches(dataset_name)

    def list_datasets(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT dataset_name, table_name, search_column, primary_key_column, created_at
                FROM datasets
                ORDER BY dataset_name
                """
            ).fetchall()

            if not rows and self._table_exists(conn, "movies"):
                self.register_dataset("movies", "movies", "primaryTitle", "tconst")
                rows = conn.execute(
                    """
                    SELECT dataset_name, table_name, search_column, primary_key_column, created_at
                    FROM datasets
                    ORDER BY dataset_name
                    """
                ).fetchall()

        out = []
        for row in rows:
            item = dict(row)
            item.setdefault("label", "Dataset 1" if item["dataset_name"] == "movies" else item["dataset_name"])
            out.append(item)
        return out

    def get_dataset_info(self, dataset_name: str) -> Dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT dataset_name, table_name, search_column, primary_key_column, created_at
                FROM datasets
                WHERE dataset_name = ?
                """,
                (dataset_name,),
            ).fetchone()

            if row is None and dataset_name == "movies" and self._table_exists(conn, "movies"):
                conn.execute(
                    """
                    INSERT INTO datasets (dataset_name, table_name, search_column, primary_key_column, created_at)
                    VALUES (?, ?, ?, ?, datetime('now'))
                    ON CONFLICT(dataset_name) DO UPDATE SET
                        table_name = excluded.table_name,
                        search_column = excluded.search_column,
                        primary_key_column = excluded.primary_key_column
                    """,
                    ("movies", "movies", "primaryTitle", "tconst"),
                )
                row = conn.execute(
                    """
                    SELECT dataset_name, table_name, search_column, primary_key_column, created_at
                    FROM datasets
                    WHERE dataset_name = ?
                    """,
                    (dataset_name,),
                ).fetchone()

        if row is None:
            raise DatasetNotRegisteredError(f"Dataset '{dataset_name}' is not registered.")
        return dict(row)

    def get_columns(self, dataset_name: str) -> List[ColumnMeta]:
        info = self.get_dataset_info(dataset_name)
        schema = self._introspect_table(info["table_name"])
        metas: List[ColumnMeta] = []
        for col in schema:
            name = col["name"]
            normalized = self._normalize_declared_type(col["type"])
            is_pk = name == info["primary_key_column"]
            is_search = name == info["search_column"]
            metas.append(
                ColumnMeta(
                    name=name,
                    declared_type=col["type"] or "",
                    normalized_type=normalized,
                    is_primary_key=is_pk,
                    is_search_column=is_search,
                    is_filterable=name in SUPPORTED_FILTER_COLUMNS,
                    is_editable=not is_pk,
                    value_kind="numeric" if name in NUMERIC_FILTER_COLUMNS else "categorical",
                )
            )
        return metas

    # ----------------------------
    # Query config / filter choices
    # ----------------------------
    def get_query_config(self, dataset_name: str) -> Dict[str, Any]:
        cached = self._query_config_cache.get(dataset_name)
        if cached is not None:
            return cached

        info = self.get_dataset_info(dataset_name)
        columns = self.get_columns(dataset_name)
        filter_columns = [
            {
                "name": col.name,
                "declared_type": col.declared_type,
                "normalized_type": col.normalized_type,
                "value_kind": col.value_kind,
            }
            for col in columns
            if col.is_filterable
        ]

        # Prefetch only the supported fast filter columns.
        prefetched: Dict[str, Any] = {}
        with self._connect() as conn:
            working_table = self._make_sample_table(conn, info["table_name"], PREFETCH_SAMPLE_RATIO)
            for col in SUPPORTED_FILTER_COLUMNS:
                if any(fc["name"] == col for fc in filter_columns):
                    prefetched[col] = self._get_filter_choices_for_table(
                        conn,
                        dataset_name,
                        working_table,
                        col,
                        label_search=None,
                        bucket_count=5,
                    )
                    self._filter_choice_cache[(dataset_name, col)] = prefetched[col]

        config = {
            "dataset_name": info["dataset_name"],
            "table_name": info["table_name"],
            "search_column": info["search_column"],
            "primary_key_column": info["primary_key_column"],
            "filter_columns": filter_columns,
            "filter_choices_by_column": prefetched,
            "create_fields": [col.name for col in columns if col.is_editable],
            "row_preview_columns": [
                "tconst",
                "primaryTitle",
                "titleType",
                "startYear",
                "averageRating",
                "numVotes",
            ],
        }
        self._query_config_cache[dataset_name] = config
        return config

    def get_dataset_query_config(self, dataset_name: str) -> Dict[str, Any]:
        return self.get_query_config(dataset_name)

    def get_query_screen_info(self, dataset_name: str) -> Dict[str, Any]:
        return self.get_query_config(dataset_name)

    def get_filter_choices(
        self,
        dataset_name: str,
        column_name: str,
        label_search: Optional[str] = None,
        bucket_count: int = 5,
    ) -> Dict[str, Any]:
        if column_name not in SUPPORTED_FILTER_COLUMNS:
            raise ValidationError(
                f"Column '{column_name}' is not a supported filter column. Supported: {', '.join(SUPPORTED_FILTER_COLUMNS)}"
            )

        cached = self._filter_choice_cache.get((dataset_name, column_name))
        if cached is not None and not label_search:
            return cached

        info = self.get_dataset_info(dataset_name)
        with self._connect() as conn:
            working_table = self._make_sample_table(conn, info["table_name"], PREFETCH_SAMPLE_RATIO)
            result = self._get_filter_choices_for_table(
                conn,
                dataset_name,
                working_table,
                column_name,
                label_search=label_search,
                bucket_count=bucket_count,
            )

        if not label_search:
            self._filter_choice_cache[(dataset_name, column_name)] = result
        return result

    # ----------------------------
    # Row querying / detail / eval
    # ----------------------------
    def query_rows(
        self,
        dataset_name: str,
        search_text: Optional[str] = None,
        filters: Optional[List[Dict[str, Any]]] = None,
        sort_by_evaluation: bool = True,
        limit: int = 20,
        offset: int = 0,
    ) -> Dict[str, Any]:
        info = self.get_dataset_info(dataset_name)
        table = self._quote_identifier(info["table_name"])
        quoted_pk = self._quote_identifier(info["primary_key_column"])

        where_clauses: List[str] = []
        params: List[Any] = []

        for flt in filters or []:
            column = flt.get("column")
            if not column:
                continue
            if column not in SUPPORTED_FILTER_COLUMNS:
                continue
            qcol = self._quote_identifier(column)

            if flt.get("kind") == "categorical":
                value = flt.get("value")
                if column == "genres":
                    where_clauses.append(
                        "(" +
                        f"{qcol} = ? OR {qcol} LIKE ? OR {qcol} LIKE ? OR {qcol} LIKE ?" +
                        ")"
                    )
                    params.extend([value, f"{value},%", f"%,{value}", f"%,{value},%"])
                else:
                    where_clauses.append(f"{qcol} = ?")
                    params.append(value)
            elif flt.get("kind") == "numeric":
                min_val = flt.get("min")
                max_val = flt.get("max")
                if min_val is None or max_val is None:
                    raise ValidationError(f"Numeric filter for '{column}' requires min and max.")
                where_clauses.append(f"{qcol} BETWEEN ? AND ?")
                params.extend([min_val, max_val])
            else:
                raise ValidationError(f"Unsupported filter kind for '{column}'.")

        sql = f"SELECT * FROM {table}"
        if where_clauses:
            sql += "\nWHERE " + "\n  AND ".join(where_clauses)
        sql += "\nLIMIT ?"
        params.append(max(limit * 10, QUERY_FETCH_CAP))

        with self._connect() as conn:
            rows = [dict(r) for r in conn.execute(sql, params).fetchall()]

        rows = self._apply_search_filter(rows, search_text or "")

        eval_map: Dict[str, Any] = {}
        if sort_by_evaluation:
            for row in rows:
                ev = self._evaluate_row(row)
                row["evaluation_score"] = ev["score"]
                row["evaluation_summary"] = ev["summary"]
                eval_map[str(row[info["primary_key_column"]])] = ev
            rows.sort(key=lambda r: (-float(r.get("evaluation_score") or 0.0), (r.get("primaryTitle") or ""), str(r.get(info["primary_key_column"]) or "")))
        else:
            rows.sort(key=lambda r: ((r.get("primaryTitle") or ""), str(r.get(info["primary_key_column"]) or "")))

        total_after = len(rows)
        page_rows = rows[offset: offset + max(1, int(limit))]
        query_token = uuid.uuid4().hex
        self._query_eval_cache[query_token] = {
            "dataset_name": dataset_name,
            "rows_by_id": eval_map,
        }

        return {
            "dataset_name": dataset_name,
            "search_column": info["search_column"],
            "sort_by_evaluation": sort_by_evaluation,
            "row_count": len(page_rows),
            "total_matching_count": total_after,
            "query_token": query_token,
            "rows": page_rows,
        }

    def query_row_candidates(
        self,
        dataset_name: str,
        search_text: Optional[str] = None,
        filters: Optional[List[Dict[str, Any]]] = None,
        sort_by_evaluation: bool = True,
        limit: int = 20,
        offset: int = 0,
    ) -> Dict[str, Any]:
        return self.query_rows(dataset_name, search_text, filters, sort_by_evaluation, limit, offset)

    def get_row_detail(self, dataset_name: str, row_id: str, query_token: Optional[str] = None) -> Dict[str, Any]:
        info = self.get_dataset_info(dataset_name)
        table = self._quote_identifier(info["table_name"])
        pk = self._quote_identifier(info["primary_key_column"])
        with self._connect() as conn:
            row = conn.execute(f"SELECT * FROM {table} WHERE {pk} = ? LIMIT 1", (row_id,)).fetchone()
        if row is None:
            raise RowNotFoundError(f"Row '{row_id}' not found in dataset '{dataset_name}'.")
        row_dict = dict(row)

        evaluation = None
        if query_token and query_token in self._query_eval_cache:
            evaluation = self._query_eval_cache[query_token].get("rows_by_id", {}).get(str(row_id))
        if evaluation is None:
            evaluation = self._evaluate_row(row_dict)

        return {
            "dataset_name": dataset_name,
            "row_id": row_id,
            "row": row_dict,
            "evaluation": evaluation,
        }

    # ----------------------------
    # CRUD
    # ----------------------------
    def create_row(self, dataset_name: str, row_data: Dict[str, Any]) -> Dict[str, Any]:
        info = self.get_dataset_info(dataset_name)
        if info["table_name"] != "movies":
            raise ValidationError("This simplified DB layer only supports the movies dataset.")

        values = dict(row_data)
        if not values.get(info["primary_key_column"]):
            values[info["primary_key_column"]] = self._next_tconst()

        allowed_columns = {c.name for c in self.get_columns(dataset_name)}
        cols = [c for c in values.keys() if c in allowed_columns]
        if not cols:
            raise ValidationError("No valid fields provided for create_row.")

        placeholders = ", ".join(["?"] * len(cols))
        col_sql = ", ".join(self._quote_identifier(c) for c in cols)
        table = self._quote_identifier(info["table_name"])
        params = [values[c] for c in cols]

        with self._connect() as conn:
            conn.execute(f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders})", params)

        self._invalidate_dataset_caches(dataset_name)
        return {"created": True, "row_id": values[info["primary_key_column"]]}

    def update_row(self, dataset_name: str, row_id: str, row_data: Dict[str, Any]) -> Dict[str, Any]:
        info = self.get_dataset_info(dataset_name)
        allowed = {c.name for c in self.get_columns(dataset_name) if c.is_editable}
        updates = {k: v for k, v in row_data.items() if k in allowed}
        if not updates:
            raise ValidationError("No editable fields provided for update_row.")

        set_sql = ", ".join(f"{self._quote_identifier(k)} = ?" for k in updates.keys())
        params = list(updates.values()) + [row_id]
        table = self._quote_identifier(info["table_name"])
        pk = self._quote_identifier(info["primary_key_column"])
        with self._connect() as conn:
            cur = conn.execute(f"UPDATE {table} SET {set_sql} WHERE {pk} = ?", params)
            if cur.rowcount == 0:
                raise RowNotFoundError(f"Row '{row_id}' not found in dataset '{dataset_name}'.")

        self._invalidate_dataset_caches(dataset_name)
        return {"updated": True, "row_id": row_id}

    def delete_row(self, dataset_name: str, row_id: str) -> Dict[str, Any]:
        info = self.get_dataset_info(dataset_name)
        table = self._quote_identifier(info["table_name"])
        pk = self._quote_identifier(info["primary_key_column"])
        with self._connect() as conn:
            cur = conn.execute(f"DELETE FROM {table} WHERE {pk} = ?", (row_id,))
            if cur.rowcount == 0:
                raise RowNotFoundError(f"Row '{row_id}' not found in dataset '{dataset_name}'.")

        self._invalidate_dataset_caches(dataset_name)
        return {"deleted": True, "row_id": row_id}

    def recompute_all_row_evaluations(self, dataset_name: str) -> Dict[str, Any]:
        return {"dataset_name": dataset_name, "recomputed_count": 0, "note": "Evaluation is computed on demand in memory."}

    # ----------------------------
    # Internals
    # ----------------------------
    def _invalidate_dataset_caches(self, dataset_name: str) -> None:
        self._query_config_cache.pop(dataset_name, None)
        for key in list(self._filter_choice_cache.keys()):
            if key[0] == dataset_name:
                self._filter_choice_cache.pop(key, None)
        for key in list(self._query_eval_cache.keys()):
            if self._query_eval_cache[key].get("dataset_name") == dataset_name:
                self._query_eval_cache.pop(key, None)

    def _introspect_table(self, table_name: str) -> List[Dict[str, Any]]:
        self._validate_identifier(table_name)
        with self._connect() as conn:
            rows = conn.execute(f"PRAGMA table_info({self._quote_identifier(table_name)})").fetchall()
        if not rows:
            raise ValidationError(f"Table '{table_name}' does not exist or has no schema.")
        return [
            {
                "cid": row["cid"],
                "name": row["name"],
                "type": row["type"] or "",
                "notnull": row["notnull"],
                "default_value": row["dflt_value"],
                "pk": row["pk"],
            }
            for row in rows
        ]

    def _table_exists(self, conn: sqlite3.Connection, table_name: str) -> bool:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1",
            (table_name,),
        ).fetchone()
        return row is not None

    def _normalize_declared_type(self, declared_type: str) -> str:
        raw = (declared_type or "").strip().upper()
        if "INT" in raw:
            return "INTEGER"
        if any(token in raw for token in ("REAL", "FLOA", "DOUB")):
            return "REAL"
        if any(token in raw for token in ("NUMERIC", "DECIMAL")):
            return "NUMERIC"
        if any(token in raw for token in ("CHAR", "CLOB", "TEXT", "VARCHAR")):
            return "TEXT"
        return "TEXT" if raw == "" else raw

    def _validate_identifier(self, identifier: str) -> None:
        if not _IDENTIFIER_RE.match(identifier):
            raise InvalidIdentifierError(f"Unsafe or invalid SQL identifier: '{identifier}'")

    def _quote_identifier(self, identifier: str) -> str:
        self._validate_identifier(identifier)
        return f'"{identifier}"'

    def _make_sample_table(self, conn: sqlite3.Connection, table_name: str, sample_ratio: float) -> str:
        sample_ratio = max(0.01, min(1.0, sample_ratio))
        conn.execute("DROP TABLE IF EXISTS temp.sampled_movies")
        conn.execute(
            f"""
            CREATE TEMP TABLE sampled_movies AS
            SELECT *
            FROM {self._quote_identifier(table_name)}
            WHERE (abs(random()) / 9223372036854775808.0) < ?
            """,
            (sample_ratio,),
        )
        return "sampled_movies"

    def _attach_percentages(self, choices: List[Dict[str, Any]], total: int) -> List[Dict[str, Any]]:
        total = int(total or 0)
        for choice in choices:
            count = int(choice.get("count", 0) or 0)
            choice["percentage"] = round((count / total) * 100.0, 2) if total > 0 else 0.0
        return choices

    def _get_filter_choices_for_table(
        self,
        conn: sqlite3.Connection,
        dataset_name: str,
        table_name: str,
        column_name: str,
        label_search: Optional[str],
        bucket_count: int,
    ) -> Dict[str, Any]:
        if column_name == "titleType":
            rows = conn.execute(
                f"""
                SELECT CAST(titleType AS TEXT) AS label, COUNT(*) AS freq
                FROM {table_name}
                WHERE titleType IS NOT NULL
                  AND TRIM(CAST(titleType AS TEXT)) <> ''
                GROUP BY titleType
                ORDER BY freq DESC, label ASC
                """
            ).fetchall()
            total = conn.execute(
                f"""
                SELECT COUNT(*)
                FROM {table_name}
                WHERE titleType IS NOT NULL
                  AND TRIM(CAST(titleType AS TEXT)) <> ''
                """
            ).fetchone()[0]
            choices = [{"label": r["label"], "value": r["label"], "count": r["freq"]} for r in rows]
            choices = self._attach_percentages(choices, total)
            if label_search:
                choices = [c for c in choices if label_search.lower() in str(c["label"]).lower()]
            return {"dataset_name": dataset_name, "column": column_name, "value_kind": "categorical", "total": int(total), "choices": choices}

        if column_name == "isAdult":
            rows = conn.execute(
                f"""
                SELECT CAST(isAdult AS TEXT) AS label, COUNT(*) AS freq
                FROM {table_name}
                WHERE isAdult IN (0, 1)
                GROUP BY isAdult
                ORDER BY freq DESC, label ASC
                """
            ).fetchall()
            total = conn.execute(
                f"""
                SELECT COUNT(*)
                FROM {table_name}
                WHERE isAdult IN (0, 1)
                """
            ).fetchone()[0]
            choices = [{"label": r["label"], "value": int(r["label"]), "count": r["freq"]} for r in rows]
            choices = self._attach_percentages(choices, total)
            if label_search:
                choices = [c for c in choices if label_search.lower() in str(c["label"]).lower()]
            return {"dataset_name": dataset_name, "column": column_name, "value_kind": "categorical", "total": int(total), "choices": choices}

        if column_name == "genres":
            rows = conn.execute(
                f"""
                WITH RECURSIVE split(rest, token) AS (
                    SELECT genres || ',', ''
                    FROM {table_name}
                    WHERE genres IS NOT NULL
                      AND TRIM(genres) <> ''
                    UNION ALL
                    SELECT
                        substr(rest, instr(rest, ',') + 1),
                        trim(substr(rest, 1, instr(rest, ',') - 1))
                    FROM split
                    WHERE rest <> ''
                )
                SELECT token AS label, COUNT(*) AS freq
                FROM split
                WHERE token <> ''
                GROUP BY token
                ORDER BY freq DESC, label ASC
                """
            ).fetchall()
            total = conn.execute(
                f"""
                WITH RECURSIVE split(rest, token) AS (
                    SELECT genres || ',', ''
                    FROM {table_name}
                    WHERE genres IS NOT NULL
                      AND TRIM(genres) <> ''
                    UNION ALL
                    SELECT
                        substr(rest, instr(rest, ',') + 1),
                        trim(substr(rest, 1, instr(rest, ',') - 1))
                    FROM split
                    WHERE rest <> ''
                )
                SELECT COUNT(*)
                FROM split
                WHERE token <> ''
                """
            ).fetchone()[0]
            choices = [{"label": r["label"], "value": r["label"], "count": r["freq"]} for r in rows]
            choices = self._attach_percentages(choices, total)
            if label_search:
                choices = [c for c in choices if label_search.lower() in str(c["label"]).lower()]
            return {"dataset_name": dataset_name, "column": column_name, "value_kind": "categorical", "total": int(total), "choices": choices}

        if column_name == "startYear":
            return self._profile_start_year(conn, dataset_name, table_name)
        if column_name == "runtimeMinutes":
            return self._profile_runtime_minutes(conn, dataset_name, table_name)
        if column_name == "averageRating":
            return self._profile_average_rating(conn, dataset_name, table_name)
        if column_name == "numVotes":
            return self._profile_num_votes(conn, dataset_name, table_name)

        raise ValidationError(f"Unsupported fast filter column: {column_name}")

    def _profile_start_year(self, conn: sqlite3.Connection, dataset_name: str, table_name: str) -> Dict[str, Any]:
        mn, mx = conn.execute(
            f"SELECT MIN(startYear), MAX(startYear) FROM {table_name} WHERE startYear IS NOT NULL"
        ).fetchone()
        total = conn.execute(
            f"SELECT COUNT(*) FROM {table_name} WHERE startYear IS NOT NULL"
        ).fetchone()[0]
        if mn is None or mx is None:
            return {"dataset_name": dataset_name, "column": "startYear", "value_kind": "numeric", "bucket_count": 0, "choices": []}
        if mn == mx:
            choices = self._attach_percentages([{"label": f"[{mn}, {mx}]", "min": mn, "max": mx, "count": total}], total)
            return {
                "dataset_name": dataset_name,
                "column": "startYear",
                "value_kind": "numeric",
                "bucket_count": 1,
                "total": int(total),
                "choices": choices,
            }
        width = (mx - mn) / 5.0
        boundaries = [mn + width * i for i in range(1, 5)]
        raw_rows = conn.execute(
            f"""
            WITH bucketed AS (
                SELECT CASE
                    WHEN startYear < ? THEN 0
                    WHEN startYear < ? THEN 1
                    WHEN startYear < ? THEN 2
                    WHEN startYear < ? THEN 3
                    ELSE 4
                END AS bucket_idx
                FROM {table_name}
                WHERE startYear IS NOT NULL
            )
            SELECT bucket_idx, COUNT(*) AS freq
            FROM bucketed
            GROUP BY bucket_idx
            """,
            tuple(boundaries),
        ).fetchall()
        choices = []
        for bucket_idx, freq in raw_rows:
            lo = mn + bucket_idx * width
            hi = mx if bucket_idx == 4 else (mn + (bucket_idx + 1) * width)
            label = f"[{int(lo)}, {int(hi)}]" if bucket_idx == 4 else f"[{int(lo)}, {int(hi)})"
            choices.append({"label": label, "min": int(lo), "max": int(hi), "count": freq})
        choices.sort(key=lambda x: (-x["count"], x["label"]))
        choices = self._attach_percentages(choices, total)
        return {"dataset_name": dataset_name, "column": "startYear", "value_kind": "numeric", "bucket_count": 5, "total": int(total), "choices": choices}

    def _profile_runtime_minutes(self, conn: sqlite3.Connection, dataset_name: str, table_name: str) -> Dict[str, Any]:
        total = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM {table_name}
            WHERE runtimeMinutes IS NOT NULL
              AND runtimeMinutes >= 0
            """
        ).fetchone()[0]
        raw_rows = conn.execute(
            f"""
            WITH bucketed AS (
                SELECT CASE
                    WHEN runtimeMinutes >= 510 THEN 17
                    ELSE CAST(runtimeMinutes / 30 AS INTEGER)
                END AS bucket_idx
                FROM {table_name}
                WHERE runtimeMinutes IS NOT NULL
                  AND runtimeMinutes >= 0
            )
            SELECT bucket_idx, COUNT(*) AS freq
            FROM bucketed
            GROUP BY bucket_idx
            """
        ).fetchall()
        choices = []
        for bucket_idx, freq in raw_rows:
            if bucket_idx == 17:
                label = "[510, inf)"
                lo, hi = 510, 10**12
            else:
                lo = bucket_idx * 30
                hi = lo + 30
                label = f"[{lo}, {hi})"
            choices.append({"label": label, "min": lo, "max": hi, "count": freq})
        choices.sort(key=lambda x: (-x["count"], x["label"]))
        choices = self._attach_percentages(choices, total)
        return {"dataset_name": dataset_name, "column": "runtimeMinutes", "value_kind": "numeric", "total": int(total), "choices": choices}

    def _profile_average_rating(self, conn: sqlite3.Connection, dataset_name: str, table_name: str) -> Dict[str, Any]:
        total = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM {table_name}
            WHERE averageRating IS NOT NULL
              AND averageRating >= 0
              AND averageRating <= 10
            """
        ).fetchone()[0]
        raw_rows = conn.execute(
            f"""
            WITH bucketed AS (
                SELECT CASE
                    WHEN averageRating = 10 THEN 19
                    ELSE CAST(averageRating / 0.5 AS INTEGER)
                END AS bucket_idx
                FROM {table_name}
                WHERE averageRating IS NOT NULL
                  AND averageRating >= 0
                  AND averageRating <= 10
            )
            SELECT bucket_idx, COUNT(*) AS freq
            FROM bucketed
            GROUP BY bucket_idx
            """
        ).fetchall()
        choices = []
        for bucket_idx, freq in raw_rows:
            lo = bucket_idx * 0.5
            hi = lo + 0.5
            label = "[9.5, 10.0]" if bucket_idx == 19 else f"[{lo:.1f}, {hi:.1f})"
            choices.append({"label": label, "min": lo, "max": hi, "count": freq})
        choices.sort(key=lambda x: (-x["count"], x["label"]))
        choices = self._attach_percentages(choices, total)
        return {"dataset_name": dataset_name, "column": "averageRating", "value_kind": "numeric", "total": int(total), "choices": choices}

    def _profile_num_votes(self, conn: sqlite3.Connection, dataset_name: str, table_name: str) -> Dict[str, Any]:
        total = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM {table_name}
            WHERE numVotes IS NOT NULL
              AND numVotes >= 0
            """
        ).fetchone()[0]
        raw_rows = conn.execute(
            f"""
            WITH bucketed AS (
                SELECT CASE
                    WHEN numVotes >= 100000 THEN 19
                    WHEN numVotes < 10000 THEN CAST(numVotes / 1000 AS INTEGER)
                    ELSE 10 + CAST((numVotes - 10000) / 10000 AS INTEGER)
                END AS bucket_idx
                FROM {table_name}
                WHERE numVotes IS NOT NULL
                  AND numVotes >= 0
            )
            SELECT bucket_idx, COUNT(*) AS freq
            FROM bucketed
            GROUP BY bucket_idx
            """
        ).fetchall()
        choices = []
        for bucket_idx, freq in raw_rows:
            if 0 <= bucket_idx <= 9:
                lo = bucket_idx * 1000
                hi = lo + 1000
                label = f"[{lo}, {hi})"
            elif 10 <= bucket_idx <= 18:
                lo = 10000 + (bucket_idx - 10) * 10000
                hi = lo + 10000
                label = f"[{lo}, {hi})"
            else:
                lo, hi = 100000, 10**15
                label = "[100000, inf)"
            choices.append({"label": label, "min": lo, "max": hi, "count": freq})
        choices.sort(key=lambda x: (-x["count"], x["label"]))
        choices = self._attach_percentages(choices, total)
        return {"dataset_name": dataset_name, "column": "numVotes", "value_kind": "numeric", "total": int(total), "choices": choices}

    def _normalize_text(self, text: str) -> str:
        return " ".join((text or "").lower().strip().split())

    def _title_matches_search(self, title: str, search: str) -> bool:
        title_n = self._normalize_text(title)
        search_n = self._normalize_text(search)
        if not title_n or not search_n:
            return False
        if search_n in title_n:
            return True
        search_tokens = [tok for tok in search_n.split() if tok]
        if any(tok in title_n for tok in search_tokens):
            return True
        ratio = SequenceMatcher(None, title_n, search_n).ratio()
        return ratio >= 0.65

    def _apply_search_filter(self, rows: List[Dict[str, Any]], search_value: str) -> List[Dict[str, Any]]:
        if not search_value:
            return rows
        return [row for row in rows if self._title_matches_search(str(row.get("primaryTitle") or ""), search_value)]

    def _evaluate_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        rating = row.get("averageRating")
        votes = row.get("numVotes")
        runtime = row.get("runtimeMinutes")
        year = row.get("startYear")

        raw_rating = self._rating_score(rating)
        raw_votes = self._votes_score(votes)
        raw_runtime = self._runtime_score(runtime)
        raw_year = self._year_score(year)

        weighted_rating = raw_rating * 0.60
        weighted_votes = raw_votes * 0.30
        weighted_runtime = raw_runtime * 0.07
        weighted_year = raw_year * 0.03
        total_score = round(weighted_rating + weighted_votes + weighted_runtime + weighted_year, 4)

        details = {
            "rating_score_raw": raw_rating,
            "rating_score_weighted": round(weighted_rating, 4),
            "numVotes_score_raw": raw_votes,
            "numVotes_score_weighted": round(weighted_votes, 4),
            "runtime_score_raw": raw_runtime,
            "runtime_score_weighted": round(weighted_runtime, 4),
            "startYear_score_raw": raw_year,
            "startYear_score_weighted": round(weighted_year, 4),
        }
        return {
            "score": total_score,
            "details": details,
            "summary": self._make_explanation(row, details),
        }

    def _rating_score(self, value: Any) -> int:
        if value is None:
            return 0
        value = float(value)
        if value >= 9.0:
            return 10
        if value >= 8.5:
            return 9
        if value >= 8.0:
            return 8
        if value >= 7.5:
            return 7
        if value >= 7.0:
            return 6
        if value >= 6.0:
            return 5
        if value >= 5.0:
            return 4
        if value >= 4.0:
            return 3
        if value >= 3.0:
            return 2
        return 1

    def _votes_score(self, value: Any) -> int:
        if value is None:
            return 0
        value = int(value)
        if value >= 100000:
            return 10
        if value >= 50000:
            return 9
        if value >= 20000:
            return 8
        if value >= 10000:
            return 7
        if value >= 5000:
            return 6
        if value >= 1000:
            return 5
        if value >= 500:
            return 4
        if value >= 100:
            return 3
        if value >= 10:
            return 2
        return 1

    def _runtime_score(self, value: Any) -> int:
        if value is None:
            return 0
        value = int(value)
        if value >= 150:
            return 5
        if value >= 120:
            return 4
        if value >= 90:
            return 3
        if value >= 60:
            return 2
        return 1

    def _year_score(self, value: Any) -> int:
        if value is None:
            return 0
        value = int(value)
        if value >= 2020:
            return 5
        if value >= 2010:
            return 4
        if value >= 2000:
            return 3
        if value >= 1990:
            return 2
        return 1

    def _make_explanation(self, row: Dict[str, Any], detail_scores: Dict[str, Any]) -> str:
        parts: List[str] = []
        title = row.get("primaryTitle") or row.get("originalTitle") or row.get("tconst")
        title_type = row.get("titleType")
        year = row.get("startYear")
        genres = row.get("genres")
        rating = row.get("averageRating")
        votes = row.get("numVotes")
        runtime = row.get("runtimeMinutes")

        intro = f'"{title}"'
        if year is not None:
            intro += f" ({year})"
        if title_type:
            intro += f" is a {title_type}"
        if genres:
            intro += f" in {genres}"
        parts.append(intro + ".")

        rs = detail_scores["rating_score_raw"]
        if rating is None:
            parts.append("It has no rating data, so the rating component contributes nothing.")
        elif rs >= 8:
            parts.append(f"It has a very strong rating of {rating}, which heavily boosts the score.")
        elif rs >= 6:
            parts.append(f"It has a solid rating of {rating}, which gives it a good boost.")
        elif rs >= 4:
            parts.append(f"It has a middling rating of {rating}, so the score boost is modest.")
        else:
            parts.append(f"It has a low rating of {rating}, which limits the score.")

        vs = detail_scores["numVotes_score_raw"]
        if votes is None:
            parts.append("It has no vote-count data, so confidence from popularity is missing.")
        elif vs >= 8:
            parts.append(f"It has a high vote count ({int(votes):,}), which adds strong confidence to the evaluation.")
        elif vs >= 5:
            parts.append(f"It has a decent vote count ({int(votes):,}), giving the score some reliability support.")
        else:
            parts.append(f"It has a low vote count ({int(votes):,}), so the score relies less on broad audience confirmation.")

        rts = detail_scores["runtime_score_raw"]
        if runtime is None:
            parts.append("Runtime is missing, so that component does not help.")
        elif rts >= 4:
            parts.append(f"Its runtime of {runtime} minutes gives it a small positive bump.")
        elif rts >= 2:
            parts.append(f"Its runtime of {runtime} minutes gives only a slight positive effect.")
        else:
            parts.append(f"Its short runtime of {runtime} minutes contributes very little to the score.")

        ys = detail_scores["startYear_score_raw"]
        if year is None:
            parts.append("Release year is missing, so recency adds nothing.")
        elif ys >= 4:
            parts.append(f"Because it is relatively recent ({year}), it gets a small recency bonus.")
        elif ys >= 2:
            parts.append(f"Its release year ({year}) adds only a minor recency effect.")
        else:
            parts.append(f"Its older release year ({year}) gives almost no recency bonus.")

        return " ".join(parts)

    def _next_tconst(self) -> str:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT tconst
                FROM movies
                WHERE tconst GLOB 'tt[0-9]*'
                ORDER BY LENGTH(tconst) DESC, tconst DESC
                LIMIT 1
                """
            ).fetchone()
        last = row[0] if row else "tt0000000"
        m = re.match(r"tt(\d+)$", str(last))
        number = int(m.group(1)) + 1 if m else 1
        width = max(7, len(str(number)), len(m.group(1)) if m else 7)
        return f"tt{number:0{width}d}"
