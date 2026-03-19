from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from django.conf import settings
from django.core.management import execute_from_command_line
from django.core.wsgi import get_wsgi_application
from django.core.asgi import get_asgi_application
from django.http import HttpRequest, JsonResponse
from django.urls import path
from django.views.decorators.csrf import csrf_exempt

# -----------------------------------------------------------------------------
# Path bootstrap
# -----------------------------------------------------------------------------

CURRENT_FILE = Path(__file__).resolve()
SERVER_DIR = CURRENT_FILE.parent
PROJECT_ROOT = SERVER_DIR.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(SERVER_DIR) not in sys.path:
    sys.path.insert(0, str(SERVER_DIR))

# -----------------------------------------------------------------------------
# Local import
# -----------------------------------------------------------------------------

try:
    from db import (
        DatabaseManager,
        DBError,
        DatasetNotRegisteredError,
        InvalidIdentifierError,
        RowNotFoundError,
        ValidationError,
    )
except ImportError:
    from server.db import (  # fallback if run as module
        DatabaseManager,
        DBError,
        DatasetNotRegisteredError,
        InvalidIdentifierError,
        RowNotFoundError,
        ValidationError,
    )

# -----------------------------------------------------------------------------
# Basic config
# -----------------------------------------------------------------------------

DB_PATH = str((SERVER_DIR / "db.db").resolve())

# Adjust these to match your actual SQLite table.
# dataset_name = logical API name
# table_name   = real SQLite table name inside db.db
DEFAULT_DATASET_NAME = os.environ.get("APP_DATASET_NAME", "movies")
DEFAULT_TABLE_NAME = os.environ.get("APP_TABLE_NAME", "movies")
DEFAULT_SEARCH_COLUMN = os.environ.get("APP_SEARCH_COLUMN", "primaryTitle")
DEFAULT_PRIMARY_KEY = os.environ.get("APP_PRIMARY_KEY", "tconst")

db = DatabaseManager(DB_PATH)


def try_register_default_dataset() -> None:
    """
    Best-effort registration so the API can work immediately.
    If your dataset is already registered, db.py will upsert it.
    If the table/columns are wrong, the server still starts and returns
    a readable error on request.
    """
    try:
        db.register_dataset(
            dataset_name=DEFAULT_DATASET_NAME,
            table_name=DEFAULT_TABLE_NAME,
            search_column=DEFAULT_SEARCH_COLUMN,
            primary_key_column=DEFAULT_PRIMARY_KEY,
        )
    except Exception as exc:
        print(f"[server_main] dataset auto-registration skipped: {exc}")


# -----------------------------------------------------------------------------
# Django setup
# -----------------------------------------------------------------------------

def configure_django() -> None:
    if settings.configured:
        return

    settings.configure(
        DEBUG=True,
        SECRET_KEY="dev-secret-key-change-me",
        ROOT_URLCONF=__name__,
        ALLOWED_HOSTS=["*"],
        MIDDLEWARE=[
            "django.middleware.common.CommonMiddleware",
        ],
        INSTALLED_APPS=[
            "django.contrib.contenttypes",
        ],
        TEMPLATES=[],
        USE_TZ=True,
    )


configure_django()

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
    
def parse_json_body(request: HttpRequest) -> Dict[str, Any]:
    if not request.body:
        return {}
    try:
        return json.loads(request.body.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise ValidationError("Request body must be valid JSON.") from exc


def get_dataset_name(request: HttpRequest, body: Dict[str, Any]) -> str:
    return (
        body.get("dataset_name")
        or request.GET.get("dataset_name")
        or DEFAULT_DATASET_NAME
    )


def success(data: Dict[str, Any], status: int = 200) -> JsonResponse:
    return JsonResponse({"ok": True, "data": data}, status=status)


def failure(
    message: str,
    *,
    error_type: str = "server_error",
    status: int = 400,
    extra: Optional[Dict[str, Any]] = None,
) -> JsonResponse:
    payload = {
        "ok": False,
        "error": {
            "type": error_type,
            "message": message,
        }
    }
    if extra:
        payload["error"]["details"] = extra
    return JsonResponse(payload, status=status)


def handle_db_error(exc: Exception) -> JsonResponse:
    if isinstance(exc, DatasetNotRegisteredError):
        return failure(str(exc), error_type="dataset_not_registered", status=404)
    if isinstance(exc, RowNotFoundError):
        return failure(str(exc), error_type="row_not_found", status=404)
    if isinstance(exc, (ValidationError, InvalidIdentifierError)):
        return failure(str(exc), error_type="validation_error", status=400)
    if isinstance(exc, DBError):
        return failure(str(exc), error_type="database_error", status=500)
    return failure(str(exc), error_type="server_error", status=500)


# -----------------------------------------------------------------------------
# API views
# -----------------------------------------------------------------------------

@csrf_exempt
def health(request: HttpRequest) -> JsonResponse:
    if request.method != "GET":
        return failure("Method not allowed.", error_type="method_not_allowed", status=405)

    return success(
        {
            "service": "dataset-api",
            "db_path": DB_PATH,
            "default_dataset_name": DEFAULT_DATASET_NAME,
            "default_table_name": DEFAULT_TABLE_NAME,
        }
    )


@csrf_exempt
def get_initial_column_detail(request: HttpRequest) -> JsonResponse:
    """
    GET /api/initial-column-detail?dataset_name=movies

    Returns:
    - dataset_name
    - table_name
    - search_column
    - primary_key_column
    - filter_columns
    """
    if request.method != "GET":
        return failure("Method not allowed.", error_type="method_not_allowed", status=405)

    try:
        dataset_name = get_dataset_name(request, {})
        data = db.get_query_screen_info(dataset_name)
        return success(data)
    except Exception as exc:
        return handle_db_error(exc)


@csrf_exempt
def get_filter_column_detail_default(request: HttpRequest) -> JsonResponse:
    """
    POST /api/filter-column-detail/default
    {
      "dataset_name": "movies",
      "column_name": "genres",
      "bucket_count": 5
    }

    No label search. Just default frequency-sorted choices / numeric buckets.
    """
    if request.method != "POST":
        return failure("Method not allowed.", error_type="method_not_allowed", status=405)

    try:
        body = parse_json_body(request)
        dataset_name = get_dataset_name(request, body)
        column_name = body["column_name"]
        bucket_count = int(body.get("bucket_count", 5))

        data = db.get_filter_choices(
            dataset_name=dataset_name,
            column_name=column_name,
            label_search=None,
            bucket_count=bucket_count,
        )
        return success(data)
    except KeyError as exc:
        return failure(f"Missing required field: {exc}", error_type="validation_error", status=400)
    except Exception as exc:
        return handle_db_error(exc)


@csrf_exempt
def get_filter_column_detail_from_search(request: HttpRequest) -> JsonResponse:
    """
    POST /api/filter-column-detail/search
    {
      "dataset_name": "movies",
      "column_name": "genres",
      "label_search": "Drama",
      "bucket_count": 5
    }

    For categorical columns this filters labels using label_search.
    For numeric columns label_search is ignored and numeric buckets are returned.
    """
    if request.method != "POST":
        return failure("Method not allowed.", error_type="method_not_allowed", status=405)

    try:
        body = parse_json_body(request)
        dataset_name = get_dataset_name(request, body)
        column_name = body["column_name"]
        label_search = body.get("label_search")
        bucket_count = int(body.get("bucket_count", 5))

        data = db.get_filter_choices(
            dataset_name=dataset_name,
            column_name=column_name,
            label_search=label_search,
            bucket_count=bucket_count,
        )
        return success(data)
    except KeyError as exc:
        return failure(f"Missing required field: {exc}", error_type="validation_error", status=400)
    except Exception as exc:
        return handle_db_error(exc)


@csrf_exempt
def get_row_candidate_from_search_and_filter(request: HttpRequest) -> JsonResponse:
    """
    POST /api/row-candidates
    {
      "dataset_name": "movies",
      "search_text": "matrix",
      "filters": [
        {"column": "genres", "kind": "categorical", "value": "Action"},
        {"column": "averageRating", "kind": "numeric", "min": 7.0, "max": 10.0}
      ],
      "sort_by_evaluation": true,
      "limit": 50,
      "offset": 0
    }
    """
    if request.method != "POST":
        return failure("Method not allowed.", error_type="method_not_allowed", status=405)

    try:
        body = parse_json_body(request)
        dataset_name = get_dataset_name(request, body)
        search_text = body.get("search_text")
        filters = body.get("filters", [])
        sort_by_evaluation = bool(body.get("sort_by_evaluation", True))
        limit = int(body.get("limit", 50))
        offset = int(body.get("offset", 0))

        data = db.query_row_candidates(
            dataset_name=dataset_name,
            search_text=search_text,
            filters=filters,
            sort_by_evaluation=sort_by_evaluation,
            limit=limit,
            offset=offset,
        )
        return success(data)
    except Exception as exc:
        return handle_db_error(exc)


# -----------------------------------------------------------------------------
# URL routes
# -----------------------------------------------------------------------------

urlpatterns = [
    path("health", health),
    path("api/initial-column-detail", get_initial_column_detail),
    path("api/filter-column-detail/default", get_filter_column_detail_default),
    path("api/filter-column-detail/search", get_filter_column_detail_from_search),
    path("api/row-candidates", get_row_candidate_from_search_and_filter),
]

# -----------------------------------------------------------------------------
# WSGI / ASGI exposure
# -----------------------------------------------------------------------------

application = get_wsgi_application()
asgi_application = get_asgi_application()

# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------

def main() -> None:
    try_register_default_dataset()

    # Optional: precompute cached evaluations on startup.
    # Safe to leave on if you want ranking ready immediately.
    try:
        db.recompute_all_row_evaluations(DEFAULT_DATASET_NAME)
    except Exception as exc:
        print(f"[server_main] evaluation precompute skipped: {exc}")

    host = os.environ.get("APP_HOST", "127.0.0.1")
    port = os.environ.get("APP_PORT", "8000")

    argv = sys.argv[:]
    if len(argv) == 1:
        argv = [argv[0], "runserver", f"{host}:{port}"]

    execute_from_command_line(argv)


if __name__ == "__main__":
    main()