# server/db.py

from __future__ import annotations

import json
import math
import re
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

# ------------------------------------------------------------------------------
# Optional config import
#
# Expected flexible shapes supported:
#
# 1) DATASET_CONFIGS = {
#        "movies": {
#            "search_column": "title",
#            "primary_key": "id",
#            "evaluation": callable
#        }
#    }
#
# 2) DATASET_EVALUATORS = {"movies": callable}
#    DATASET_SEARCH_COLUMNS = {"movies": "title"}
#    DATASET_PRIMARY_KEYS = {"movies": "id"}
#
# If your config uses a different shape, adjust _resolve_* helpers below.
# ------------------------------------------------------------------------------

try:
    from config.analysis_evaluation_search import (  # type: ignore
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


# ------------------------------------------------------------------------------
# Data helpers
# ------------------------------------------------------------------------------

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
    value_kind: str  # "numeric" | "categorical"


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


# ------------------------------------------------------------------------------
# Main DB manager
# ------------------------------------------------------------------------------

class DatabaseManager:
    """
    Row CRUD + cached evaluation manager.

    This class assumes:
    - your actual dataset rows live in normal SQLite tables
    - internal metadata/evaluation tables live alongside them
    - dataset-specific search/evaluation config is resolved from config
    """

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._ensure_internal_tables()

    # --------------------------------------------------------------------------
    # Connection helpers
    # --------------------------------------------------------------------------

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

    # --------------------------------------------------------------------------
    # Registration / dataset metadata
    # --------------------------------------------------------------------------

    def register_dataset(
        self,
        dataset_name: str,
        table_name: str,
        search_column: Optional[str] = None,
        primary_key_column: Optional[str] = None,
    ) -> None:
        """
        Register an existing SQLite table as a dataset managed by this module.
        """
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
                ORDER BY dataset_name ASC
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
            raise DatasetNotRegisteredError(
                f"Dataset '{dataset_name}' is not registered."
            )

        return dict(row)

    # --------------------------------------------------------------------------
    # Schema / UI screen metadata
    # --------------------------------------------------------------------------

    def get_query_screen_info(self, dataset_name: str) -> Dict[str, Any]:
        """
        Returns the metadata UI needs for the read/update/delete query screen:
        - designated search column
        - all filterable columns
        - column type classification
        """
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

    def get_columns(self, dataset_name: str) -> List[ColumnMeta]:
        info = self.get_dataset_info(dataset_name)
        table_name = info["table_name"]
        search_column = info["search_column"]
        primary_key_column = info["primary_key_column"]

        schema = self._introspect_table(table_name)
        metas: List[ColumnMeta] = []

        for col in schema:
            name = col["name"]
            normalized = self._normalize_declared_type(col["type"])
            is_pk = name == primary_key_column
            is_search = name == search_column
            value_kind = self._infer_value_kind(normalized)
            is_filterable = (not is_pk) and (not is_search)
            is_editable = not is_pk

            metas.append(
                ColumnMeta(
                    name=name,
                    declared_type=col["type"] or "",
                    normalized_type=normalized,
                    is_primary_key=is_pk,
                    is_search_column=is_search,
                    is_filterable=is_filterable,
                    is_editable=is_editable,
                    value_kind=value_kind,
                )
            )

        return metas

    def get_create_update_columns(self, dataset_name: str) -> List[Dict[str, Any]]:
        return [
            {
                "name": col.name,
                "declared_type": col.declared_type,
                "normalized_type": col.normalized_type,
                "value_kind": col.value_kind,
            }
            for col in self.get_columns(dataset_name)
            if col.is_editable
        ]

    # --------------------------------------------------------------------------
    # Filter option retrieval
    # --------------------------------------------------------------------------

    def get_filter_choices(
        self,
        dataset_name: str,
        column_name: str,
        label_search: Optional[str] = None,
        bucket_count: int = 5,
    ) -> Dict[str, Any]:
        """
        For a categorical column:
            returns labels with frequency, optionally filtered by label_search

        For a numeric column:
            returns bucket ranges with frequency
        """
        info = self.get_dataset_info(dataset_name)
        table_name = info["table_name"]

        column_meta = self._get_column_meta(dataset_name, column_name)
        quoted_column = self._quote_identifier(column_meta.name)
        quoted_table = self._quote_identifier(table_name)

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

        numeric_type = column_meta.normalized_type
        if numeric_type not in {"INTEGER", "REAL", "NUMERIC"}:
            raise ValidationError(
                f"Column '{column_name}' is not bucketable as numeric."
            )

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
        bucket_width = (max_value - min_value) / bucket_count
        buckets: List[Dict[str, Any]] = []

        with self._connect() as conn:
            for i in range(bucket_count):
                bucket_min = min_value + i * bucket_width
                bucket_max = max_value if i == bucket_count - 1 else min_value + (i + 1) * bucket_width

                if i == bucket_count - 1:
                    count_row = conn.execute(
                        f"""
                        SELECT COUNT(*) AS c
                        FROM {quoted_table}
                        WHERE {quoted_column} >= ? AND {quoted_column} <= ?
                        """,
                        (bucket_min, bucket_max),
                    ).fetchone()
                else:
                    count_row = conn.execute(
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
                        "count": count_row["c"],
                    }
                )

        return {
            "column": column_name,
            "value_kind": "numeric",
            "bucket_count": bucket_count,
            "choices": buckets,
        }

    # --------------------------------------------------------------------------
    # Candidate row query
    # --------------------------------------------------------------------------

    def query_row_candidates(
        self,
        dataset_name: str,
        search_text: Optional[str] = None,
        filters: Optional[List[Dict[str, Any]]] = None,
        sort_by_evaluation: bool = True,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        filters format:
        [
            {"column": "genre", "kind": "categorical", "value": "Drama"},
            {"column": "rating", "kind": "numeric", "min": 8.0, "max": 10.0}
        ]
        """
        info = self.get_dataset_info(dataset_name)
        table_name = info["table_name"]
        search_column = info["search_column"]
        pk_column = info["primary_key_column"]

        quoted_table = self._quote_identifier(table_name)
        quoted_pk = self._quote_identifier(pk_column)
        quoted_search = self._quote_identifier(search_column)

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
                        f"Numeric filter for column '{column}' requires 'min' and 'max'."
                    )
                where_clauses.append(f"t.{quoted_col} >= ? AND t.{quoted_col} <= ?")
                params.extend([min_val, max_val])

            else:
                raise ValidationError(f"Unsupported filter kind for column '{column}'.")

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        order_sql = (
            "ORDER BY ev.score DESC, t.{pk} ASC".format(pk=quoted_pk)
            if sort_by_evaluation
            else "ORDER BY t.{pk} ASC".format(pk=quoted_pk)
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
            "search_column": search_column,
            "sort_by_evaluation": sort_by_evaluation,
            "row_count": len(rows),
            "rows": [self._row_to_dict(row) for row in rows], 
        }

    # --------------------------------------------------------------------------
    # Row detail
    # --------------------------------------------------------------------------

    def get_row_detail(self, dataset_name: str, row_id: Any) -> Dict[str, Any]:
        info = self.get_dataset_info(dataset_name)
        table_name = info["table_name"]
        pk_column = info["primary_key_column"]

        quoted_table = self._quote_identifier(table_name)
        quoted_pk = self._quote_identifier(pk_column)

        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT *
                FROM {quoted_table}
                WHERE {quoted_pk} = ?
                """,
                (row_id,),
            ).fetchone()

            if row is None:
                raise RowNotFoundError(
                    f"Row '{row_id}' was not found in dataset '{dataset_name}'."
                )

            ev = conn.execute(
                """
                SELECT score, details_json, updated_at
                FROM row_evaluation
                WHERE dataset_name = ? AND row_id = ?
                """,
                (dataset_name, str(row_id)),
            ).fetchone()

        return {
            "dataset_name": dataset_name,
            "row": dict(row),
            "evaluation": (
                {
                    "score": ev["score"],
                    "details": self._safe_json_loads(ev["details_json"]),
                    "updated_at": ev["updated_at"],
                }
                if ev is not None
                else None
            ),
        }

    # --------------------------------------------------------------------------
    # CRUD
    # --------------------------------------------------------------------------

    def create_row(self, dataset_name: str, row_data: Dict[str, Any]) -> Dict[str, Any]:
        info = self.get_dataset_info(dataset_name)
        table_name = info["table_name"]
        pk_column = info["primary_key_column"]

        editable_columns = {c.name: c for c in self.get_columns(dataset_name) if c.is_editable}
        cleaned_data = self._validate_and_clean_row_input(row_data, editable_columns)

        if not cleaned_data:
            raise ValidationError("No valid editable fields were provided for row creation.")

        columns = list(cleaned_data.keys())
        values = [cleaned_data[c] for c in columns]

        quoted_table = self._quote_identifier(table_name)
        quoted_cols = ", ".join(self._quote_identifier(c) for c in columns)
        placeholders = ", ".join("?" for _ in columns)

        with self._connect() as conn:
            cur = conn.execute(
                f"""
                INSERT INTO {quoted_table} ({quoted_cols})
                VALUES ({placeholders})
                """,
                values,
            )

            new_row_id = cur.lastrowid
            if not new_row_id and pk_column in cleaned_data:
                new_row_id = cleaned_data[pk_column]

        self.recompute_all_row_evaluations(dataset_name)
        return self.get_row_detail(dataset_name, new_row_id)

    def update_row(
        self,
        dataset_name: str,
        row_id: Any,
        updates: Dict[str, Any],
    ) -> Dict[str, Any]:
        info = self.get_dataset_info(dataset_name)
        table_name = info["table_name"]
        pk_column = info["primary_key_column"]

        editable_columns = {c.name: c for c in self.get_columns(dataset_name) if c.is_editable}
        cleaned_updates = self._validate_and_clean_row_input(updates, editable_columns)

        if not cleaned_updates:
            raise ValidationError("No valid editable fields were provided for row update.")

        quoted_table = self._quote_identifier(table_name)
        quoted_pk = self._quote_identifier(pk_column)

        set_clause = ", ".join(
            f"{self._quote_identifier(col)} = ?" for col in cleaned_updates.keys()
        )
        values = list(cleaned_updates.values())

        with self._connect() as conn:
            cur = conn.execute(
                f"""
                UPDATE {quoted_table}
                SET {set_clause}
                WHERE {quoted_pk} = ?
                """,
                [*values, row_id],
            )
            if cur.rowcount == 0:
                raise RowNotFoundError(
                    f"Row '{row_id}' was not found in dataset '{dataset_name}'."
                )

        self.recompute_all_row_evaluations(dataset_name)
        return self.get_row_detail(dataset_name, row_id)

    def delete_row(self, dataset_name: str, row_id: Any) -> Dict[str, Any]:
        info = self.get_dataset_info(dataset_name)
        table_name = info["table_name"]
        pk_column = info["primary_key_column"]

        # Capture existing detail before deletion for UI confirmation/history.
        existing = self.get_row_detail(dataset_name, row_id)

        quoted_table = self._quote_identifier(table_name)
        quoted_pk = self._quote_identifier(pk_column)

        with self._connect() as conn:
            cur = conn.execute(
                f"""
                DELETE FROM {quoted_table}
                WHERE {quoted_pk} = ?
                """,
                (row_id,),
            )
            if cur.rowcount == 0:
                raise RowNotFoundError(
                    f"Row '{row_id}' was not found in dataset '{dataset_name}'."
                )

            conn.execute(
                """
                DELETE FROM row_evaluation
                WHERE dataset_name = ? AND row_id = ?
                """,
                (dataset_name, str(row_id)),
            )

        self.recompute_all_row_evaluations(dataset_name)
        return {
            "dataset_name": dataset_name,
            "deleted": True,
            "row_id": row_id,
            "deleted_row": existing,
        }

    # --------------------------------------------------------------------------
    # Evaluation
    # --------------------------------------------------------------------------

    def recompute_all_row_evaluations(self, dataset_name: str) -> Dict[str, Any]:
        """
        Recompute cached scores for all rows in one dataset.

        Since you decided evaluation is cached and completed up front, this is the
        central refresh function for startup and for post-mutation refreshes.
        """
        info = self.get_dataset_info(dataset_name)
        table_name = info["table_name"]
        pk_column = info["primary_key_column"]

        evaluator = self._resolve_evaluator(dataset_name)
        quoted_table = self._quote_identifier(table_name)
        quoted_pk = self._quote_identifier(pk_column)

        with self._connect() as conn:
            rows = conn.execute(f"SELECT * FROM {quoted_table}").fetchall()
            all_rows = [dict(r) for r in rows]

        # Evaluator contract:
        #   evaluator(row_dict, all_row_dicts, db_manager) -> (score, details_dict)
        #
        # details_dict can be any JSON-serializable dict.
        payloads: List[Tuple[str, float, str, str]] = []

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
                """
                DELETE FROM row_evaluation
                WHERE dataset_name = ?
                """,
                (dataset_name,),
            )

            conn.executemany(
                """
                INSERT INTO row_evaluation (
                    dataset_name, row_id, score, details_json, updated_at
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                payloads,
            )

        return {
            "dataset_name": dataset_name,
            "recomputed_count": len(payloads),
        }

    def get_row_evaluation(self, dataset_name: str, row_id: Any) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT score, details_json, updated_at
                FROM row_evaluation
                WHERE dataset_name = ? AND row_id = ?
                """,
                (dataset_name, str(row_id)),
            ).fetchone()

        if row is None:
            return None

        return {
            "score": row["score"],
            "details": self._safe_json_loads(row["details_json"]),
            "updated_at": row["updated_at"],
        }

    # --------------------------------------------------------------------------
    # Internal helpers
    # --------------------------------------------------------------------------

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
        if dataset_name in DATASET_CONFIGS:
            cfg = DATASET_CONFIGS[dataset_name]
            if "search_column" in cfg:
                return cfg["search_column"]

        if dataset_name in DATASET_SEARCH_COLUMNS:
            return DATASET_SEARCH_COLUMNS[dataset_name]

        raise ValidationError(
            f"No search column config found for dataset '{dataset_name}'."
        )

    def _resolve_primary_key(
        self,
        dataset_name: str,
        schema: List[Dict[str, Any]],
    ) -> str:
        if dataset_name in DATASET_CONFIGS:
            cfg = DATASET_CONFIGS[dataset_name]
            if "primary_key" in cfg:
                return cfg["primary_key"]

        if dataset_name in DATASET_PRIMARY_KEYS:
            return DATASET_PRIMARY_KEYS[dataset_name]

        pk_cols = [col["name"] for col in schema if col["pk"]]
        if pk_cols:
            return pk_cols[0]

        # common fallback
        column_names = [col["name"] for col in schema]
        if "id" in column_names:
            return "id"

        raise ValidationError(
            f"Could not determine primary key for dataset '{dataset_name}'."
        )

    def _resolve_evaluator(
        self,
        dataset_name: str,
    ) -> Callable[[Dict[str, Any], List[Dict[str, Any]], "DatabaseManager"], Tuple[float, Dict[str, Any]]]:
        if dataset_name in DATASET_CONFIGS:
            cfg = DATASET_CONFIGS[dataset_name]
            fn = cfg.get("evaluation")
            if callable(fn):
                return fn

        fn = DATASET_EVALUATORS.get(dataset_name)
        if callable(fn):
            return fn

        # Safe fallback evaluator so the system still works before config is ready.
        def _fallback_evaluator(
            row: Dict[str, Any],
            all_rows: List[Dict[str, Any]],
            db: "DatabaseManager",
        ) -> Tuple[float, Dict[str, Any]]:
            return 0.0, {"warning": "No evaluator configured for this dataset."}

        return _fallback_evaluator

    def _get_column_meta(self, dataset_name: str, column_name: str) -> ColumnMeta:
        for col in self.get_columns(dataset_name):
            if col.name == column_name:
                return col
        raise ValidationError(
            f"Column '{column_name}' does not exist in dataset '{dataset_name}'."
        )

    def _validate_and_clean_row_input(
        self,
        data: Dict[str, Any],
        allowed_columns: Dict[str, ColumnMeta],
    ) -> Dict[str, Any]:
        cleaned: Dict[str, Any] = {}

        for key, value in data.items():
            if key not in allowed_columns:
                continue

            meta = allowed_columns[key]
            cleaned[key] = self._coerce_value(value, meta)

        return cleaned

    def _coerce_value(self, value: Any, meta: ColumnMeta) -> Any:
        if value is None:
            return None

        t = meta.normalized_type

        try:
            if t == "INTEGER":
                if value == "":
                    return None
                return int(value)

            if t in {"REAL", "NUMERIC"}:
                if value == "":
                    return None
                return float(value)

            if t == "BOOLEAN":
                if isinstance(value, bool):
                    return int(value)
                if isinstance(value, (int, float)):
                    return int(bool(value))
                if isinstance(value, str):
                    lowered = value.strip().lower()
                    if lowered in {"1", "true", "yes", "y", "on"}:
                        return 1
                    if lowered in {"0", "false", "no", "n", "off"}:
                        return 0
                raise ValidationError(f"Invalid boolean value for column '{meta.name}'.")

            # TEXT / fallback
            return str(value)

        except (TypeError, ValueError) as exc:
            raise ValidationError(
                f"Invalid value '{value}' for column '{meta.name}' ({meta.normalized_type})."
            ) from exc

    def _normalize_declared_type(self, declared_type: str) -> str:
        raw = (declared_type or "").strip().upper()

        # SQLite affinity-friendly normalization
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

        # SQLite tables often omit types.
        return "TEXT" if raw == "" else raw

    def _infer_value_kind(self, normalized_type: str) -> str:
        return "numeric" if normalized_type in {"INTEGER", "REAL", "NUMERIC"} else "categorical"

    def _validate_identifier(self, identifier: str) -> None:
        if not _IDENTIFIER_RE.match(identifier):
            raise InvalidIdentifierError(
                f"Unsafe or invalid SQL identifier: '{identifier}'"
            )

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


# ------------------------------------------------------------------------------
# Example fallback evaluator you can move into config/analysis_evaluation_search.py
# ------------------------------------------------------------------------------

def example_movie_evaluator(
    row: Dict[str, Any],
    all_rows: List[Dict[str, Any]],
    db: DatabaseManager,
) -> Tuple[float, Dict[str, Any]]:
    """
    Very simple demo evaluator.

    You should move dataset-specific evaluators into config/analysis_evaluation_search.py.
    This is just here so db.py is usable immediately for testing.
    """
    # Example fields; safely ignore if missing.
    rating_values = [
        float(r["rating"])
        for r in all_rows
        if r.get("rating") is not None
    ]
    runtime_values = [
        float(r["runtime"])
        for r in all_rows
        if r.get("runtime") is not None
    ]

    rating = float(row["rating"]) if row.get("rating") is not None else None
    runtime = float(row["runtime"]) if row.get("runtime") is not None else None

    rating_percentile = 0.0
    if rating is not None and rating_values:
        less_or_equal = sum(1 for x in rating_values if x <= rating)
        rating_percentile = less_or_equal / len(rating_values)

    runtime_closeness = 0.0
    if runtime is not None and runtime_values:
        avg_runtime = sum(runtime_values) / len(runtime_values)
        max_dist = max(abs(x - avg_runtime) for x in runtime_values) or 1.0
        runtime_closeness = 1.0 - min(abs(runtime - avg_runtime) / max_dist, 1.0)

    score = (0.7 * rating_percentile) + (0.3 * runtime_closeness)

    details = {
        "rating_percentile": rating_percentile,
        "runtime_closeness": runtime_closeness,
        "formula": {
            "rating_percentile_weight": 0.7,
            "runtime_closeness_weight": 0.3,
        },
    }

    return round(score, 6), details


# ------------------------------------------------------------------------------
# Example usage
# ------------------------------------------------------------------------------

if __name__ == "__main__":
    db = DatabaseManager("server/db.db")

    # Example:
    # db.register_dataset(
    #     dataset_name="movies",
    #     table_name="movies",
    #     search_column="title",
    #     primary_key_column="id",
    # )
    #
    # print(db.get_query_screen_info("movies"))
    # print(db.get_filter_choices("movies", "genre"))
    # print(db.get_filter_choices("movies", "rating", bucket_count=5))
    # print(db.query_row_candidates("movies", search_text="matrix"))
    #
    # db.recompute_all_row_evaluations("movies")