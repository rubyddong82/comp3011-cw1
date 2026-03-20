from __future__ import annotations

import json
import math
import re
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

try:
    from config.search_analysis import (  # type: ignore
        DATASET_CONFIGS,
        DATASET_EVALUATORS,
        DATASET_PRIMARY_KEYS,
        DATASET_SEARCH_COLUMNS,
    )
except Exception:
    DATASET_CONFIGS = {}
    DATASET_EVALUATORS = {}
    DATASET_PRIMARY_KEYS = {}
    DATASET_SEARCH_COLUMNS = {}


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


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
        self._ensure_internal_tables()

    @contextmanager
    def _connect(self) -> Iterable[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
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
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS row_evaluation (
                    dataset_name TEXT NOT NULL,
                    row_id TEXT NOT NULL,
                    score REAL NOT NULL,
                    details_json TEXT,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (dataset_name, row_id),
                    FOREIGN KEY (dataset_name) REFERENCES datasets(dataset_name)
                        ON DELETE CASCADE
                )
                """
            )

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

        resolved_search = search_column or self._resolve_search_column(dataset_name)
        resolved_pk = primary_key_column or self._resolve_primary_key(dataset_name, schema)

        if resolved_search not in column_names:
            raise ValidationError(
                f"Search column '{resolved_search}' does not exist in table '{table_name}'."
            )
        if resolved_pk not in column_names:
            raise ValidationError(
                f"Primary key column '{resolved_pk}' does not exist in table '{table_name}'."
            )

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO datasets (
                    dataset_name, table_name, search_column, primary_key_column, created_at
                )
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(dataset_name) DO UPDATE SET
                    table_name = excluded.table_name,
                    search_column = excluded.search_column,
                    primary_key_column = excluded.primary_key_column
                """,
                (
                    dataset_name,
                    table_name,
                    resolved_search,
                    resolved_pk,
                    self._utc_now(),
                ),
            )

    def list_datasets(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT dataset_name, table_name, search_column, primary_key_column, created_at
                FROM datasets
                ORDER BY dataset_name
                """
            ).fetchall()
        return [dict(row) for row in rows]

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
                    is_filterable=(not is_pk) and (not is_search),
                    is_editable=not is_pk,
                    value_kind=self._infer_value_kind(normalized),
                )
            )
        return metas

    def get_query_screen_info(self, dataset_name: str) -> Dict[str, Any]:
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
        return {
            "dataset_name": info["dataset_name"],
            "table_name": info["table_name"],
            "search_column": info["search_column"],
            "primary_key_column": info["primary_key_column"],
            "filter_columns": filter_columns,
        }

    def get_filter_choices(
        self,
        dataset_name: str,
        column_name: str,
        label_search: Optional[str] = None,
        bucket_count: int = 5,
    ) -> Dict[str, Any]:
        info = self.get_dataset_info(dataset_name)
        column_meta = self._get_column_meta(dataset_name, column_name)
        quoted_table = self._quote_identifier(info["table_name"])
        quoted_column = self._quote_identifier(column_meta.name)

        if column_meta.value_kind == "categorical":
            query = f"""
                SELECT {quoted_column} AS label, COUNT(*) AS freq
                FROM {quoted_table}
                WHERE {quoted_column} IS NOT NULL
            """
            params: List[Any] = []

            if label_search:
                query += f" AND CAST({quoted_column} AS TEXT) LIKE ? "
                params.append(f"%{label_search}%")

            query += f"""
                GROUP BY {quoted_column}
                ORDER BY freq DESC, label ASC
            """

            with self._connect() as conn:
                rows = conn.execute(query, params).fetchall()

            return {
                "column": column_name,
                "value_kind": "categorical",
                "choices": [
                    {"label": row["label"], "count": row["freq"]}
                    for row in rows
                ],
            }

        if column_meta.normalized_type not in {"INTEGER", "REAL", "NUMERIC"}:
            raise ValidationError(f"Column '{column_name}' is not numeric.")

        with self._connect() as conn:
            stats = conn.execute(
                f"""
                SELECT
                    MIN({quoted_column}) AS min_value,
                    MAX({quoted_column}) AS max_value,
                    COUNT({quoted_column}) AS non_null_count
                FROM {quoted_table}
                """
            ).fetchone()

        min_value = stats["min_value"]
        max_value = stats["max_value"]
        non_null_count = stats["non_null_count"]

        if non_null_count == 0 or min_value is None or max_value is None:
            return {
                "column": column_name,
                "value_kind": "numeric",
                "bucket_count": bucket_count,
                "choices": [],
            }

        if min_value == max_value:
            return {
                "column": column_name,
                "value_kind": "numeric",
                "bucket_count": 1,
                "choices": [
                    {
                        "label": f"{min_value} - {max_value}",
                        "min": min_value,
                        "max": max_value,
                        "count": int(non_null_count),
                    }
                ],
            }

        bucket_count = max(1, int(bucket_count))
        width = (max_value - min_value) / bucket_count
        buckets: List[Dict[str, Any]] = []

        with self._connect() as conn:
            for i in range(bucket_count):
                bucket_min = min_value + i * width
                bucket_max = max_value if i == bucket_count - 1 else min_value + (i + 1) * width

                if i == bucket_count - 1:
                    row = conn.execute(
                        f"""
                        SELECT COUNT(*) AS c
                        FROM {quoted_table}
                        WHERE {quoted_column} >= ? AND {quoted_column} <= ?
                        """,
                        (bucket_min, bucket_max),
                    ).fetchone()
                else:
                    row = conn.execute(
                        f"""
                        SELECT COUNT(*) AS c
                        FROM {quoted_table}
                        WHERE {quoted_column} >= ? AND {quoted_column} < ?
                        """,
                        (bucket_min, bucket_max),
                    ).fetchone()

                buckets.append(
                    {
                        "label": f"{self._fmt_num(bucket_min)} - {self._fmt_num(bucket_max)}",
                        "min": bucket_min,
                        "max": bucket_max,
                        "count": row["c"],
                    }
                )

        return {
            "column": column_name,
            "value_kind": "numeric",
            "bucket_count": bucket_count,
            "choices": buckets,
        }

    def query_row_candidates(
        self,
        dataset_name: str,
        search_text: Optional[str] = None,
        filters: Optional[List[Dict[str, Any]]] = None,
        sort_by_evaluation: bool = True,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        info = self.get_dataset_info(dataset_name)
        quoted_table = self._quote_identifier(info["table_name"])
        quoted_pk = self._quote_identifier(info["primary_key_column"])
        quoted_search = self._quote_identifier(info["search_column"])

        where_clauses: List[str] = []
        params: List[Any] = []

        if search_text:
            where_clauses.append(f"CAST(t.{quoted_search} AS TEXT) LIKE ?")
            params.append(f"%{search_text}%")

        for flt in filters or []:
            column = flt["column"]
            meta = self._get_column_meta(dataset_name, column)
            quoted_col = self._quote_identifier(meta.name)

            if flt.get("kind") == "categorical":
                where_clauses.append(f"t.{quoted_col} = ?")
                params.append(flt["value"])
            elif flt.get("kind") == "numeric":
                min_val = flt.get("min")
                max_val = flt.get("max")
                if min_val is None or max_val is None:
                    raise ValidationError(
                        f"Numeric filter for '{column}' requires min and max."
                    )
                where_clauses.append(f"t.{quoted_col} >= ? AND t.{quoted_col} <= ?")
                params.extend([min_val, max_val])
            else:
                raise ValidationError(f"Unsupported filter kind for '{column}'.")

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        order_sql = (
            f"ORDER BY ev.score DESC, t.{quoted_pk} ASC"
            if sort_by_evaluation
            else f"ORDER BY t.{quoted_pk} ASC"
        )

        limit = max(1, int(limit))
        offset = max(0, int(offset))

        query = f"""
            SELECT
                t.*,
                ev.score AS evaluation_score,
                ev.details_json AS evaluation_details_json,
                ev.updated_at AS evaluation_updated_at
            FROM {quoted_table} AS t
            LEFT JOIN row_evaluation AS ev
                ON ev.dataset_name = ?
               AND ev.row_id = CAST(t.{quoted_pk} AS TEXT)
            {where_sql}
            {order_sql}
            LIMIT ? OFFSET ?
        """

        final_params = [dataset_name, *params, limit, offset]

        with self._connect() as conn:
            rows = conn.execute(query, final_params).fetchall()

        return {
            "dataset_name": dataset_name,
            "search_column": info["search_column"],
            "sort_by_evaluation": sort_by_evaluation,
            "row_count": len(rows),
            "rows": [self._row_to_dict(row) for row in rows],
        }

    def recompute_all_row_evaluations(self, dataset_name: str) -> Dict[str, Any]:
        info = self.get_dataset_info(dataset_name)
        quoted_table = self._quote_identifier(info["table_name"])
        pk_column = info["primary_key_column"]
        evaluator = self._resolve_evaluator(dataset_name)

        with self._connect() as conn:
            rows = conn.execute(f"SELECT * FROM {quoted_table}").fetchall()
            all_rows = [dict(row) for row in rows]

        payloads: List[Tuple[str, str, float, str, str]] = []
        for row in all_rows:
            score, details = evaluator(row, all_rows, self)
            payloads.append(
                (
                    dataset_name,
                    str(row[pk_column]),
                    float(score),
                    json.dumps(details or {}, ensure_ascii=False),
                    self._utc_now(),
                )
            )

        with self._connect() as conn:
            conn.execute(
                "DELETE FROM row_evaluation WHERE dataset_name = ?",
                (dataset_name,),
            )
            if payloads:
                conn.executemany(
                    """
                    INSERT INTO row_evaluation (
                        dataset_name, row_id, score, details_json, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    payloads,
                )

        return {"dataset_name": dataset_name, "recomputed_count": len(payloads)}

    def _introspect_table(self, table_name: str) -> List[Dict[str, Any]]:
        self._validate_identifier(table_name)

        with self._connect() as conn:
            rows = conn.execute(
                f"PRAGMA table_info({self._quote_identifier(table_name)})"
            ).fetchall()

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

    def _resolve_search_column(self, dataset_name: str) -> str:
        if dataset_name in DATASET_CONFIGS and "search_column" in DATASET_CONFIGS[dataset_name]:
            return DATASET_CONFIGS[dataset_name]["search_column"]
        if dataset_name in DATASET_SEARCH_COLUMNS:
            return DATASET_SEARCH_COLUMNS[dataset_name]
        raise ValidationError(f"No search column config found for dataset '{dataset_name}'.")

    def _resolve_primary_key(self, dataset_name: str, schema: List[Dict[str, Any]]) -> str:
        if dataset_name in DATASET_CONFIGS and "primary_key" in DATASET_CONFIGS[dataset_name]:
            return DATASET_CONFIGS[dataset_name]["primary_key"]
        if dataset_name in DATASET_PRIMARY_KEYS:
            return DATASET_PRIMARY_KEYS[dataset_name]

        pk_cols = [col["name"] for col in schema if col["pk"]]
        if pk_cols:
            return pk_cols[0]

        column_names = [col["name"] for col in schema]
        if "id" in column_names:
            return "id"

        raise ValidationError(f"Could not determine primary key for dataset '{dataset_name}'.")

    def _resolve_evaluator(
        self,
        dataset_name: str,
    ) -> Callable[[Dict[str, Any], List[Dict[str, Any]], "DatabaseManager"], Tuple[float, Dict[str, Any]]]:
        if dataset_name in DATASET_CONFIGS:
            fn = DATASET_CONFIGS[dataset_name].get("evaluation")
            if callable(fn):
                return fn

        fn = DATASET_EVALUATORS.get(dataset_name)
        if callable(fn):
            return fn

        def fallback_evaluator(
            row: Dict[str, Any],
            all_rows: List[Dict[str, Any]],
            db: "DatabaseManager",
        ) -> Tuple[float, Dict[str, Any]]:
            return 0.0, {"warning": "No evaluator configured for this dataset."}

        return fallback_evaluator

    def _get_column_meta(self, dataset_name: str, column_name: str) -> ColumnMeta:
        for col in self.get_columns(dataset_name):
            if col.name == column_name:
                return col
        raise ValidationError(
            f"Column '{column_name}' does not exist in dataset '{dataset_name}'."
        )

    def _normalize_declared_type(self, declared_type: str) -> str:
        raw = (declared_type or "").strip().upper()
        if "INT" in raw:
            return "INTEGER"
        if any(token in raw for token in ("REAL", "FLOA", "DOUB")):
            return "REAL"
        if any(token in raw for token in ("NUMERIC", "DECIMAL")):
            return "NUMERIC"
        if "BOOL" in raw:
            return "BOOLEAN"
        if any(token in raw for token in ("CHAR", "CLOB", "TEXT", "VARCHAR")):
            return "TEXT"
        return "TEXT" if raw == "" else raw

    def _infer_value_kind(self, normalized_type: str) -> str:
        return "numeric" if normalized_type in {"INTEGER", "REAL", "NUMERIC"} else "categorical"

    def _validate_identifier(self, identifier: str) -> None:
        if not _IDENTIFIER_RE.match(identifier):
            raise InvalidIdentifierError(f"Unsafe or invalid SQL identifier: '{identifier}'")

    def _quote_identifier(self, identifier: str) -> str:
        self._validate_identifier(identifier)
        return f'"{identifier}"'

    def _row_to_dict(self, row: sqlite3.Row) -> Dict[str, Any]:
        output = dict(row)
        if "evaluation_details_json" in output:
            output["evaluation_details"] = self._safe_json_loads(output.pop("evaluation_details_json"))
        return output

    def _safe_json_loads(self, value: Optional[str]) -> Any:
        if not value:
            return None
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return {"raw": value}

    def _utc_now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _fmt_num(self, value: float) -> str:
        if value is None:
            return "null"
        if math.isclose(value, round(value)):
            return str(int(round(value)))
        return f"{value:.2f}"


def example_movie_evaluator(
    row: Dict[str, Any],
    all_rows: List[Dict[str, Any]],
    db: DatabaseManager,
) -> Tuple[float, Dict[str, Any]]:
    rating_values = [
        float(r["averageRating"])
        for r in all_rows
        if r.get("averageRating") is not None
    ]

    rating = float(row["averageRating"]) if row.get("averageRating") is not None else None

    rating_percentile = 0.0
    if rating is not None and rating_values:
        less_or_equal = sum(1 for x in rating_values if x <= rating)
        rating_percentile = less_or_equal / len(rating_values)

    return round(rating_percentile, 6), {
        "rating_percentile": rating_percentile,
        "formula": "relative position of averageRating within dataset"
    }
