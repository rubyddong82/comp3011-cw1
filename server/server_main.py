from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional
from wsgiref.simple_server import WSGIRequestHandler, make_server

from django.conf import settings
from django.core.wsgi import get_wsgi_application
from django.http import HttpRequest, JsonResponse
from django.urls import path
from django.views.decorators.csrf import csrf_exempt

CURRENT_FILE = Path(__file__).resolve()
SERVER_DIR = CURRENT_FILE.parent
PROJECT_ROOT = SERVER_DIR.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(SERVER_DIR) not in sys.path:
    sys.path.insert(0, str(SERVER_DIR))

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
    from server.db import (
        DatabaseManager,
        DBError,
        DatasetNotRegisteredError,
        InvalidIdentifierError,
        RowNotFoundError,
        ValidationError,
    )

DB_PATH = str((SERVER_DIR / "imdb.db").resolve())
DEFAULT_DATASET_NAME = os.environ.get("APP_DATASET_NAME", "movies")
DEFAULT_TABLE_NAME = os.environ.get("APP_TABLE_NAME", "movies_small")
DEFAULT_SEARCH_COLUMN = os.environ.get("APP_SEARCH_COLUMN", "primaryTitle")
DEFAULT_PRIMARY_KEY = os.environ.get("APP_PRIMARY_KEY", "tconst")

db = DatabaseManager(DB_PATH)


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


def parse_json_body(request: HttpRequest) -> Dict[str, Any]:
    if not request.body:
        return {}
    try:
        return json.loads(request.body.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise ValidationError("Request body must be valid JSON.") from exc


def success(data: Dict[str, Any], status: int = 200) -> JsonResponse:
    return JsonResponse({"ok": True, "data": data}, status=status)


def failure(
    message: str,
    *,
    error_type: str = "server_error",
    status: int = 400,
    extra: Optional[Dict[str, Any]] = None,
) -> JsonResponse:
    payload: Dict[str, Any] = {
        "ok": False,
        "error": {
            "type": error_type,
            "message": message,
        },
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


def try_register_default_dataset() -> None:
    try:
        db.register_dataset(
            dataset_name=DEFAULT_DATASET_NAME,
            table_name=DEFAULT_TABLE_NAME,
            search_column=DEFAULT_SEARCH_COLUMN,
            primary_key_column=DEFAULT_PRIMARY_KEY,
        )
    except Exception as exc:
        print(f"[server_main] dataset auto-registration skipped: {exc}")


def call_first_available(*names: str, **kwargs: Any) -> Any:
    """
    Allows server_main.py to work with slightly different DBManager method names.
    """
    last_missing = None
    for name in names:
        fn = getattr(db, name, None)
        if callable(fn):
            return fn(**kwargs)
        last_missing = name
    raise AttributeError(f"No compatible DB method found. Tried: {', '.join(names)}")


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


# -----------------------------
# New dataset-style API
# -----------------------------


@csrf_exempt
def get_output_logs(request: HttpRequest) -> JsonResponse:
    if request.method != "GET":
        return failure("Method not allowed.", error_type="method_not_allowed", status=405)

    try:
        fn = getattr(db, "get_output_logs", None)
        if not callable(fn):
            return success({"logs": []})
        return success({"logs": fn()})
    except Exception as exc:
        return handle_db_error(exc)

@csrf_exempt
def list_datasets(request: HttpRequest) -> JsonResponse:
    if request.method != "GET":
        return failure("Method not allowed.", error_type="method_not_allowed", status=405)

    try:
        data = call_first_available("list_datasets", "get_datasets")
        # normalize shape a bit
        if isinstance(data, list):
            return success({"datasets": data})
        if isinstance(data, dict) and "datasets" in data:
            return success(data)
        return success({"datasets": data if isinstance(data, list) else []})
    except Exception as exc:
        return handle_db_error(exc)


@csrf_exempt
def get_dataset_query_config(request: HttpRequest, dataset_name: str) -> JsonResponse:
    if request.method != "GET":
        return failure("Method not allowed.", error_type="method_not_allowed", status=405)

    try:
        data = call_first_available(
            "get_query_config",
            "get_dataset_query_config",
            "get_query_screen_info",
            dataset_name=dataset_name,
        )
        return success(data)
    except Exception as exc:
        return handle_db_error(exc)


@csrf_exempt
def get_filter_choices(request: HttpRequest, dataset_name: str, column_name: str) -> JsonResponse:
    if request.method != "GET":
        return failure("Method not allowed.", error_type="method_not_allowed", status=405)

    try:
        label_search = request.GET.get("label_search")
        bucket_count = int(request.GET.get("bucket_count", "5"))

        data = call_first_available(
            "get_filter_choices",
            dataset_name=dataset_name,
            column_name=column_name,
            label_search=label_search,
            bucket_count=bucket_count,
        )
        return success(data)
    except Exception as exc:
        return handle_db_error(exc)


@csrf_exempt
def query_rows(request: HttpRequest, dataset_name: str) -> JsonResponse:
    if request.method != "POST":
        return failure("Method not allowed.", error_type="method_not_allowed", status=405)

    try:
        body = parse_json_body(request)

        data = call_first_available(
            "query_rows",
            "query_row_candidates",
            dataset_name=dataset_name,
            search_text=body.get("search_text", ""),
            filters=body.get("filters", []),
            sort_by_evaluation=bool(body.get("sort_by_evaluation", True)),
            limit=int(body.get("limit", 20)),
            offset=int(body.get("offset", 0)),
        )
        return success(data)
    except Exception as exc:
        return handle_db_error(exc)


@csrf_exempt
def get_row_detail(request: HttpRequest, dataset_name: str, row_id: str) -> JsonResponse:
    if request.method != "GET":
        return failure("Method not allowed.", error_type="method_not_allowed", status=405)

    try:
        query_token = request.GET.get("query_token")
        data = call_first_available(
            "get_row_detail",
            dataset_name=dataset_name,
            row_id=row_id,
            query_token=query_token,
        )
        return success(data)
    except Exception as exc:
        return handle_db_error(exc)


@csrf_exempt
def create_row(request: HttpRequest, dataset_name: str) -> JsonResponse:
    if request.method != "POST":
        return failure("Method not allowed.", error_type="method_not_allowed", status=405)

    try:
        body = parse_json_body(request)
        row_data = body.get("row", body)
        data = call_first_available(
            "create_row",
            dataset_name=dataset_name,
            row_data=row_data,
        )
        return success(data, status=201)
    except Exception as exc:
        return handle_db_error(exc)


@csrf_exempt
def update_row(request: HttpRequest, dataset_name: str, row_id: str) -> JsonResponse:
    if request.method not in {"PUT", "PATCH", "POST"}:
        return failure("Method not allowed.", error_type="method_not_allowed", status=405)

    try:
        body = parse_json_body(request)
        row_data = body.get("row", body)
        data = call_first_available(
            "update_row",
            dataset_name=dataset_name,
            row_id=row_id,
            row_data=row_data,
        )
        return success(data)
    except Exception as exc:
        return handle_db_error(exc)


@csrf_exempt
def delete_row(request: HttpRequest, dataset_name: str, row_id: str) -> JsonResponse:
    if request.method not in {"DELETE", "POST"}:
        return failure("Method not allowed.", error_type="method_not_allowed", status=405)

    try:
        data = call_first_available(
            "delete_row",
            dataset_name=dataset_name,
            row_id=row_id,
        )
        return success(data)
    except Exception as exc:
        return handle_db_error(exc)


# -----------------------------
# Backward-compatible aliases
# -----------------------------

@csrf_exempt
def get_initial_column_detail(request: HttpRequest) -> JsonResponse:
    if request.method != "GET":
        return failure("Method not allowed.", error_type="method_not_allowed", status=405)

    dataset_name = request.GET.get("dataset_name", DEFAULT_DATASET_NAME)
    return get_dataset_query_config(request, dataset_name)


@csrf_exempt
def get_filter_column_detail_default(request: HttpRequest) -> JsonResponse:
    if request.method != "POST":
        return failure("Method not allowed.", error_type="method_not_allowed", status=405)

    try:
        body = parse_json_body(request)
        dataset_name = body.get("dataset_name", DEFAULT_DATASET_NAME)
        column_name = body["column_name"]
        bucket_count = int(body.get("bucket_count", 5))

        data = call_first_available(
            "get_filter_choices",
            dataset_name=dataset_name,
            column_name=column_name,
            label_search=None,
            bucket_count=bucket_count,
        )
        return success(data)
    except KeyError as exc:
        return failure(
            f"Missing required field: {exc}",
            error_type="validation_error",
            status=400,
        )
    except Exception as exc:
        return handle_db_error(exc)


@csrf_exempt
def get_filter_column_detail_from_search(request: HttpRequest) -> JsonResponse:
    if request.method != "POST":
        return failure("Method not allowed.", error_type="method_not_allowed", status=405)

    try:
        body = parse_json_body(request)
        dataset_name = body.get("dataset_name", DEFAULT_DATASET_NAME)
        column_name = body["column_name"]
        label_search = body.get("label_search")
        bucket_count = int(body.get("bucket_count", 5))

        data = call_first_available(
            "get_filter_choices",
            dataset_name=dataset_name,
            column_name=column_name,
            label_search=label_search,
            bucket_count=bucket_count,
        )
        return success(data)
    except KeyError as exc:
        return failure(
            f"Missing required field: {exc}",
            error_type="validation_error",
            status=400,
        )
    except Exception as exc:
        return handle_db_error(exc)


@csrf_exempt
def get_row_candidate_from_search_and_filter(request: HttpRequest) -> JsonResponse:
    if request.method != "POST":
        return failure("Method not allowed.", error_type="method_not_allowed", status=405)

    try:
        body = parse_json_body(request)
        dataset_name = body.get("dataset_name", DEFAULT_DATASET_NAME)

        data = call_first_available(
            "query_rows",
            "query_row_candidates",
            dataset_name=dataset_name,
            search_text=body.get("search_text", ""),
            filters=body.get("filters", []),
            sort_by_evaluation=bool(body.get("sort_by_evaluation", True)),
            limit=int(body.get("limit", 20)),
            offset=int(body.get("offset", 0)),
        )
        return success(data)
    except Exception as exc:
        return handle_db_error(exc)


urlpatterns = [
    path("health", health),

    # new routes
    path("api/datasets", list_datasets),
    path("api/output-logs", get_output_logs),
    path("api/datasets/<str:dataset_name>/query-config", get_dataset_query_config),
    path("api/datasets/<str:dataset_name>/filters/<str:column_name>/choices", get_filter_choices),
    path("api/datasets/<str:dataset_name>/rows/query", query_rows),
    path("api/datasets/<str:dataset_name>/rows/<str:row_id>", get_row_detail),
    path("api/datasets/<str:dataset_name>/rows/create", create_row),
    path("api/datasets/<str:dataset_name>/rows/<str:row_id>/update", update_row),
    path("api/datasets/<str:dataset_name>/rows/<str:row_id>/delete", delete_row),

    # compatibility routes
    path("api/initial-column-detail", get_initial_column_detail),
    path("api/filter-column-detail/default", get_filter_column_detail_default),
    path("api/filter-column-detail/search", get_filter_column_detail_from_search),
    path("api/row-candidates", get_row_candidate_from_search_and_filter),
]

application = get_wsgi_application()


class QuietHandler(WSGIRequestHandler):
    def log_message(self, format: str, *args: object) -> None:
        return


def main(host: str = "127.0.0.1", port: int = 8000) -> None:
    try_register_default_dataset()

    try:
        recompute = getattr(db, "recompute_all_row_evaluations", None)
        if callable(recompute):
            recompute(DEFAULT_DATASET_NAME)
    except Exception as exc:
        print(f"[server_main] evaluation precompute skipped: {exc}")

    print(f"[server_main] serving on http://{host}:{port}")
    httpd = make_server(host, port, application, handler_class=QuietHandler)
    httpd.serve_forever()


if __name__ == "__main__":
    main(
        host=os.environ.get("APP_HOST", "127.0.0.1"),
        port=int(os.environ.get("APP_PORT", "8000")),
    )