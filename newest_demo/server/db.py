from __future__ import annotations

import json
import math
import re
import secrets
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

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
        self.dataset_configs: Dict[str, Dict[str, Any]] = {
            "movies": {
                "dataset_name": "movies",
                "label": "Dataset 1",
                "table_name": "movies",
                "search_column": "primaryTitle",
                "primary_key_column": "tconst",
            }
        }
        self.global_stats_cache: Dict[str, Dict[str, Any]] = {}
        self.query_eval_cache: Dict[str, Dict[str, Any]] = {}
        self.query_config_cache: Dict[str, Dict[str, Any]] = {}

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

    def get_datasets(self) -> List[Dict[str, Any]]:
        datasets: List[Dict[str, Any]] = []
        for dataset_name, cfg in self.dataset_configs.items():
            self._introspect_table(cfg["table_name"])
            datasets.append(
                {
                    "dataset_name": dataset_name,
                    "label": cfg["label"],
                    "table_name": cfg["table_name"],
                    "search_column": cfg["search_column"],
                    "primary_key_column": cfg["primary_key_column"],
                }
            )
        return datasets

    def get_dataset_info(self, dataset_name: str) -> Dict[str, Any]:
        if dataset_name not in self.dataset_configs:
            raise DatasetNotRegisteredError(f"Dataset '{dataset_name}' is not registered.")
        cfg = dict(self.dataset_configs[dataset_name])
        self._introspect_table(cfg["table_name"])
        return cfg

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

    def get_query_config(self, dataset_name: str) -> Dict[str, Any]:
        if dataset_name in self.query_config_cache:
            return self.query_config_cache[dataset_name]
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
        create_fields = [
            {
                "name": col.name,
                "declared_type": col.declared_type,
                "normalized_type": col.normalized_type,
                "value_kind": col.value_kind,
                "required": False,
            }
            for col in columns
            if not col.is_primary_key
        ]
        prefetched_filter_choices: Dict[str, Dict[str, Any]] = {}
        for col in filter_columns:
            prefetched_filter_choices[col["name"]] = self.get_filter_choices(
                dataset_name,
                col["name"],
                bucket_count=5,
            )
        result = {
            "dataset_name": info["dataset_name"],
            "label": info["label"],
            "table_name": info["table_name"],
            "search_column": info["search_column"],
            "primary_key_column": info["primary_key_column"],
            "filter_columns": filter_columns,
            "prefetched_filter_choices": prefetched_filter_choices,
            "create_fields": create_fields,
            "editable_fields": create_fields,
            "row_preview_columns": [
                info["primary_key_column"],
                info["search_column"],
                "titleType",
                "startYear",
                "averageRating",
                "numVotes",
            ],
        }
        self.query_config_cache[dataset_name] = result
        return result

    def get_filter_choices(
        self,
        dataset_name: str,
        column_name: str,
        *,
        label_search: Optional[str] = None,
        bucket_count: int = 5,
    ) -> Dict[str, Any]:
        info = self.get_dataset_info(dataset_name)
        column_meta = self._get_column_meta(dataset_name, column_name)
        quoted_table = self._quote_identifier(info["table_name"])
        quoted_column = self._quote_identifier(column_meta.name)

        if column_meta.value_kind == "categorical":
            query = f"""
                SELECT {quoted_column} AS value, COUNT(*) AS freq
                FROM {quoted_table}
                WHERE {quoted_column} IS NOT NULL
            """
            params: List[Any] = []
            if label_search:
                query += f" AND CAST({quoted_column} AS TEXT) LIKE ?"
                params.append(f"%{label_search}%")
            query += f" GROUP BY {quoted_column} ORDER BY freq DESC, value ASC LIMIT 200"
            with self._connect() as conn:
                rows = conn.execute(query, params).fetchall()
            return {
                "column": column_name,
                "value_kind": "categorical",
                "bucket_count": 0,
                "choices": [
                    {"value": row["value"], "label": str(row["value"]), "count": row["freq"]}
                    for row in rows
                ],
            }

        with self._connect() as conn:
            stats = conn.execute(
                f"""
                SELECT MIN({quoted_column}) AS min_value,
                       MAX({quoted_column}) AS max_value,
                       COUNT({quoted_column}) AS non_null_count
                FROM {quoted_table}
                """
            ).fetchone()
        min_value = stats["min_value"]
        max_value = stats["max_value"]
        non_null_count = int(stats["non_null_count"] or 0)
        if non_null_count == 0 or min_value is None or max_value is None:
            return {"column": column_name, "value_kind": "numeric", "bucket_count": 0, "choices": []}

        bucket_count = max(1, int(bucket_count))
        if math.isclose(float(min_value), float(max_value)):
            return {
                "column": column_name,
                "value_kind": "numeric",
                "bucket_count": 1,
                "choices": [
                    {
                        "label": f"{self._fmt_num(float(min_value))} - {self._fmt_num(float(max_value))}",
                        "min": min_value,
                        "max": max_value,
                        "count": non_null_count,
                    }
                ],
            }

        width = (float(max_value) - float(min_value)) / bucket_count
        choices: List[Dict[str, Any]] = []
        with self._connect() as conn:
            for i in range(bucket_count):
                bucket_min = float(min_value) + i * width
                bucket_max = float(max_value) if i == bucket_count - 1 else float(min_value) + (i + 1) * width
                if i == bucket_count - 1:
                    row = conn.execute(
                        f"SELECT COUNT(*) AS c FROM {quoted_table} WHERE {quoted_column} >= ? AND {quoted_column} <= ?",
                        (bucket_min, bucket_max),
                    ).fetchone()
                else:
                    row = conn.execute(
                        f"SELECT COUNT(*) AS c FROM {quoted_table} WHERE {quoted_column} >= ? AND {quoted_column} < ?",
                        (bucket_min, bucket_max),
                    ).fetchone()
                choices.append(
                    {
                        "label": f"{self._fmt_num(bucket_min)} - {self._fmt_num(bucket_max)}",
                        "min": bucket_min,
                        "max": bucket_max,
                        "count": int(row["c"]),
                    }
                )
        return {"column": column_name, "value_kind": "numeric", "bucket_count": bucket_count, "choices": choices}

    def query_rows(
        self,
        dataset_name: str,
        *,
        search_text: Optional[str] = None,
        filters: Optional[List[Dict[str, Any]]] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> Dict[str, Any]:
        info = self.get_dataset_info(dataset_name)
        stats = self.get_global_evaluation_stats(dataset_name)
        quoted_table = self._quote_identifier(info["table_name"])
        search_col = self._quote_identifier(info["search_column"])
        filter_where: List[str] = []
        filter_params: List[Any] = []

        for flt in filters or []:
            column = flt.get("column")
            if not column:
                continue
            meta = self._get_column_meta(dataset_name, column)
            quoted_col = self._quote_identifier(meta.name)
            kind = flt.get("kind")
            if kind == "categorical":
                filter_where.append(f"{quoted_col} = ?")
                filter_params.append(flt.get("value"))
            elif kind == "numeric":
                filter_where.append(f"{quoted_col} >= ? AND {quoted_col} <= ?")
                filter_params.extend([flt.get("min"), flt.get("max")])
            else:
                raise ValidationError(f"Unsupported filter kind for '{column}'.")

        filter_sql = "WHERE " + " AND ".join(filter_where) if filter_where else ""
        search_sql = f"WHERE CAST({search_col} AS TEXT) LIKE ?" if search_text else ""
        search_params = [f"%{search_text}%"] if search_text else []

        denom = max(1.0, float(stats["max_year"] - stats["min_year"]))
        m = float(stats["votes_threshold"])
        c = float(stats["rating_mean"])
        alpha = float(stats["recency_alpha"])
        min_year = float(stats["min_year"])

        score_expr = f"""
        CASE
            WHEN averageRating IS NULL THEN 0.0
            ELSE (
                (((COALESCE(numVotes, 0) * 1.0) / (COALESCE(numVotes, 0) + {m})) * averageRating)
                + (({m} * 1.0) / (COALESCE(numVotes, 0) + {m})) * {c}
            ) * (
                1.0 + {alpha} * (
                    CASE
                        WHEN startYear IS NULL THEN 0.0
                        ELSE MIN(MAX((startYear - {min_year}) / {denom}, 0.0), 1.0)
                    END
                )
            )
        END
        """

        query = f"""
            WITH filtered AS (
                SELECT * FROM {quoted_table}
                {filter_sql}
            ),
            searched AS (
                SELECT *, {score_expr} AS evaluation_score
                FROM filtered
                {search_sql}
            )
            SELECT *
            FROM searched
            ORDER BY evaluation_score DESC, CAST({search_col} AS TEXT) ASC
            LIMIT ? OFFSET ?
        """
        params = [*filter_params, *search_params, max(1, int(limit)), max(0, int(offset))]
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
            count_query = f"""
                WITH filtered AS (
                    SELECT * FROM {quoted_table}
                    {filter_sql}
                ),
                searched AS (
                    SELECT *, {score_expr} AS evaluation_score
                    FROM filtered
                    {search_sql}
                )
                SELECT COUNT(*) AS c FROM searched
            """
            count_row = conn.execute(count_query, [*filter_params, *search_params]).fetchone()

        rows_out: List[Dict[str, Any]] = []
        query_token = self._new_query_token()
        per_row_cache: Dict[str, Any] = {}
        for row in rows:
            row_dict = dict(row)
            eval_detail = self._build_evaluation_detail(row_dict, stats, float(row_dict.get("evaluation_score") or 0.0))
            row_id = str(row_dict[info["primary_key_column"]])
            per_row_cache[row_id] = eval_detail
            row_dict["evaluation_score"] = round(eval_detail["score"], 6)
            row_dict["evaluation_summary"] = eval_detail["summary"]
            rows_out.append(row_dict)
        self.query_eval_cache[query_token] = {
            "dataset_name": dataset_name,
            "created_at": self._utc_now(),
            "rows_by_id": per_row_cache,
        }

        return {
            "dataset_name": dataset_name,
            "query_token": query_token,
            "row_count": int(count_row["c"]),
            "rows": rows_out,
        }

    def get_row_detail(self, dataset_name: str, row_id: str, *, query_token: Optional[str] = None) -> Dict[str, Any]:
        info = self.get_dataset_info(dataset_name)
        quoted_table = self._quote_identifier(info["table_name"])
        quoted_pk = self._quote_identifier(info["primary_key_column"])
        with self._connect() as conn:
            row = conn.execute(
                f"SELECT * FROM {quoted_table} WHERE {quoted_pk} = ?",
                (row_id,),
            ).fetchone()
        if row is None:
            raise RowNotFoundError(f"Row '{row_id}' was not found in dataset '{dataset_name}'.")
        row_dict = dict(row)
        evaluation = None
        if query_token:
            session = self.query_eval_cache.get(query_token)
            if session and session.get("dataset_name") == dataset_name:
                evaluation = session.get("rows_by_id", {}).get(str(row_id))
        if evaluation is None:
            stats = self.get_global_evaluation_stats(dataset_name)
            score = self._compute_evaluation_score(row_dict, stats)
            evaluation = self._build_evaluation_detail(row_dict, stats, score)
        return {
            "dataset_name": dataset_name,
            "row_id": str(row_id),
            "row": row_dict,
            "evaluation": evaluation,
        }

    def create_row(self, dataset_name: str, values: Dict[str, Any]) -> Dict[str, Any]:
        info = self.get_dataset_info(dataset_name)
        columns = self.get_columns(dataset_name)
        create_columns = [c for c in columns if not c.is_primary_key]
        converted: Dict[str, Any] = {}
        for col in create_columns:
            converted[col.name] = self._coerce_value(values.get(col.name), col)
        new_id = self._generate_next_primary_key(dataset_name)
        quoted_table = self._quote_identifier(info["table_name"])
        all_columns = [info["primary_key_column"], *[c.name for c in create_columns]]
        placeholders = ", ".join(["?"] * len(all_columns))
        quoted_cols = ", ".join(self._quote_identifier(c) for c in all_columns)
        payload = [new_id, *[converted[c.name] for c in create_columns]]
        with self._connect() as conn:
            conn.execute(
                f"INSERT INTO {quoted_table} ({quoted_cols}) VALUES ({placeholders})",
                payload,
            )
        self._clear_caches(dataset_name)
        return self.get_row_detail(dataset_name, new_id)

    def update_row(self, dataset_name: str, row_id: str, values: Dict[str, Any]) -> Dict[str, Any]:
        info = self.get_dataset_info(dataset_name)
        columns = [c for c in self.get_columns(dataset_name) if c.is_editable]
        assignments: List[str] = []
        params: List[Any] = []
        for col in columns:
            if col.name not in values:
                continue
            assignments.append(f"{self._quote_identifier(col.name)} = ?")
            params.append(self._coerce_value(values.get(col.name), col))
        if not assignments:
            raise ValidationError("No editable values were provided.")
        params.append(row_id)
        quoted_table = self._quote_identifier(info["table_name"])
        quoted_pk = self._quote_identifier(info["primary_key_column"])
        with self._connect() as conn:
            cur = conn.execute(
                f"UPDATE {quoted_table} SET {', '.join(assignments)} WHERE {quoted_pk} = ?",
                params,
            )
            if cur.rowcount == 0:
                raise RowNotFoundError(f"Row '{row_id}' was not found in dataset '{dataset_name}'.")
        self._clear_caches(dataset_name)
        return self.get_row_detail(dataset_name, row_id)

    def delete_row(self, dataset_name: str, row_id: str) -> Dict[str, Any]:
        info = self.get_dataset_info(dataset_name)
        quoted_table = self._quote_identifier(info["table_name"])
        quoted_pk = self._quote_identifier(info["primary_key_column"])
        with self._connect() as conn:
            cur = conn.execute(f"DELETE FROM {quoted_table} WHERE {quoted_pk} = ?", (row_id,))
            if cur.rowcount == 0:
                raise RowNotFoundError(f"Row '{row_id}' was not found in dataset '{dataset_name}'.")
        self._clear_caches(dataset_name)
        return {"dataset_name": dataset_name, "row_id": str(row_id), "deleted": True}

    def get_global_evaluation_stats(self, dataset_name: str) -> Dict[str, Any]:
        if dataset_name in self.global_stats_cache:
            return self.global_stats_cache[dataset_name]
        info = self.get_dataset_info(dataset_name)
        quoted_table = self._quote_identifier(info["table_name"])
        with self._connect() as conn:
            stats = conn.execute(
                f"""
                SELECT AVG(averageRating) AS rating_mean,
                       COUNT(*) AS rated_row_count,
                       MIN(startYear) AS min_year,
                       MAX(startYear) AS max_year
                FROM {quoted_table}
                WHERE averageRating IS NOT NULL
                """
            ).fetchone()
            rated_count = int(stats["rated_row_count"] or 0)
            if rated_count == 0:
                raise ValidationError("Cannot compute evaluation statistics because no rated rows exist.")
            percentile_offset = max(0, min(rated_count - 1, int(round((rated_count - 1) * 0.75))))
            threshold_row = conn.execute(
                f"SELECT COALESCE(numVotes, 0) AS numVotes FROM {quoted_table} WHERE averageRating IS NOT NULL ORDER BY COALESCE(numVotes, 0) ASC LIMIT 1 OFFSET ?",
                (percentile_offset,),
            ).fetchone()
        result = {
            "dataset_name": dataset_name,
            "rating_mean": float(stats["rating_mean"] or 0.0),
            "votes_threshold": int(threshold_row["numVotes"] or 0),
            "min_year": int(stats["min_year"] or 0),
            "max_year": int(stats["max_year"] or 0),
            "recency_alpha": 0.15,
            "rated_row_count": rated_count,
        }
        self.global_stats_cache[dataset_name] = result
        return result

    def _compute_evaluation_score(self, row: Dict[str, Any], stats: Dict[str, Any]) -> float:
        rating = row.get("averageRating")
        if rating is None:
            return 0.0
        votes = int(row.get("numVotes") or 0)
        c = float(stats["rating_mean"])
        m = float(stats["votes_threshold"])
        weighted_rating = ((votes / (votes + m)) * float(rating)) + ((m / (votes + m)) * c) if (votes + m) > 0 else 0.0
        year = row.get("startYear")
        recency_norm = 0.0
        min_year = float(stats["min_year"])
        max_year = float(stats["max_year"])
        if year is not None and max_year > min_year:
            recency_norm = min(max((float(year) - min_year) / (max_year - min_year), 0.0), 1.0)
        return weighted_rating * (1.0 + float(stats["recency_alpha"]) * recency_norm)

    def _build_evaluation_detail(self, row: Dict[str, Any], stats: Dict[str, Any], score: float) -> Dict[str, Any]:
        rating = row.get("averageRating")
        votes = int(row.get("numVotes") or 0)
        year = row.get("startYear")
        c = float(stats["rating_mean"])
        m = float(stats["votes_threshold"])
        weighted_rating = 0.0
        if rating is not None and (votes + m) > 0:
            weighted_rating = ((votes / (votes + m)) * float(rating)) + ((m / (votes + m)) * c)
        recency_norm = 0.0
        min_year = float(stats["min_year"])
        max_year = float(stats["max_year"])
        if year is not None and max_year > min_year:
            recency_norm = min(max((float(year) - min_year) / (max_year - min_year), 0.0), 1.0)
        recency_multiplier = 1.0 + float(stats["recency_alpha"]) * recency_norm
        if rating is None:
            summary = "No rating data available, so this title is pushed to the bottom of evaluation ranking."
        elif votes >= m:
            summary = "High vote support keeps this title close to its raw rating, with a modest recency boost applied."
        else:
            summary = "Lower vote support pulls this title toward the dataset mean before applying recency."
        return {
            "score": round(float(score), 6),
            "summary": summary,
            "average_rating": rating,
            "num_votes": votes,
            "global_mean_rating": round(c, 6),
            "global_votes_threshold": int(m),
            "weighted_rating": round(weighted_rating, 6),
            "recency_norm": round(recency_norm, 6),
            "recency_multiplier": round(recency_multiplier, 6),
            "final_score": round(float(score), 6),
        }

    def _generate_next_primary_key(self, dataset_name: str) -> str:
        info = self.get_dataset_info(dataset_name)
        quoted_table = self._quote_identifier(info["table_name"])
        quoted_pk = self._quote_identifier(info["primary_key_column"])
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT {quoted_pk} AS pk FROM {quoted_table} WHERE {quoted_pk} LIKE 'tt%' ORDER BY {quoted_pk} DESC LIMIT 50"
            ).fetchall()
        max_num = 0
        for row in rows:
            pk = str(row["pk"])
            if pk.startswith("tt") and pk[2:].isdigit():
                max_num = max(max_num, int(pk[2:]))
        return f"tt{max_num + 1:07d}"

    def _coerce_value(self, value: Any, col: ColumnMeta) -> Any:
        if value in (None, ""):
            return None
        if col.normalized_type == "INTEGER":
            return int(value)
        if col.normalized_type in {"REAL", "NUMERIC"}:
            return float(value)
        return str(value)

    def _clear_caches(self, dataset_name: str) -> None:
        self.global_stats_cache.pop(dataset_name, None)
        self.query_config_cache.pop(dataset_name, None)
        doomed = [token for token, sess in self.query_eval_cache.items() if sess.get("dataset_name") == dataset_name]
        for token in doomed:
            self.query_eval_cache.pop(token, None)

    def _new_query_token(self) -> str:
        return secrets.token_urlsafe(16)

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

    def _get_column_meta(self, dataset_name: str, column_name: str) -> ColumnMeta:
        for col in self.get_columns(dataset_name):
            if col.name == column_name:
                return col
        raise ValidationError(f"Column '{column_name}' does not exist in dataset '{dataset_name}'.")

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

    def _infer_value_kind(self, normalized_type: str) -> str:
        return "numeric" if normalized_type in {"INTEGER", "REAL", "NUMERIC"} else "categorical"

    def _validate_identifier(self, identifier: str) -> None:
        if not _IDENTIFIER_RE.match(identifier):
            raise InvalidIdentifierError(f"Unsafe or invalid SQL identifier: '{identifier}'")

    def _quote_identifier(self, identifier: str) -> str:
        self._validate_identifier(identifier)
        return f'"{identifier}"'

    def _utc_now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _fmt_num(self, value: float) -> str:
        if math.isclose(value, round(value)):
            return str(int(round(value)))
        return f"{value:.2f}"
