import json
import http.client
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional
from urllib.parse import quote, urlencode
from collections import deque

from client.ui import main as ui_main


class ApiClientError(Exception):
    pass


@dataclass
class SearchSessionState:
    dataset_name: str
    query_config: Optional[Dict[str, Any]] = None
    search_text: str = ""
    filters: List[Dict[str, Any]] = field(default_factory=list)
    candidate_response: Optional[Dict[str, Any]] = None
    query_token: Optional[str] = None


class ApiHttpClient:
    def __init__(self, host: str, port: int, timeout: Optional[float] = None) -> None:
        self.host = host
        self.port = port
        self.timeout = timeout
        self._log_lock = threading.Lock()
        self._logs: Deque[str] = deque(maxlen=200)

    def _log(self, text: str) -> None:
        with self._log_lock:
            ts = time.strftime("%H:%M:%S")
            for line in text.splitlines() or [""]:
                self._logs.append(f"[{ts}] {line}")

    def get_logs(self) -> List[str]:
        with self._log_lock:
            return list(self._logs)

    def clear_logs(self) -> None:
        with self._log_lock:
            self._logs.clear()

    def request(
        self,
        method: str,
        path: str,
        *,
        query: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        final_path = path
        cleaned_query = {k: v for k, v in (query or {}).items() if v is not None}
        if cleaned_query:
            final_path = f"{path}?{urlencode(cleaned_query, doseq=True)}"

        headers = {"Accept": "application/json"}
        payload = None
        if body is not None:
            payload = json.dumps(body)
            headers["Content-Type"] = "application/json"

        self._log(f"> {method.upper()} {final_path}")
        if payload:
            self._log(f"> body {payload[:1200]}")

        conn = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)
        try:
            conn.request(method.upper(), final_path, body=payload, headers=headers)
            response = conn.getresponse()
            raw = response.read().decode("utf-8")
            self._log(f"< HTTP {response.status}")
            if raw.strip():
                self._log(f"< json {raw[:1600]}")
        except (ConnectionError, OSError, http.client.HTTPException) as exc:
            self._log(f"! error {exc}")
            raise ApiClientError(
                f"Failed to communicate with server at {self.host}:{self.port}: {exc}"
            ) from exc
        finally:
            conn.close()

        try:
            parsed = json.loads(raw) if raw.strip() else {}
        except json.JSONDecodeError as exc:
            self._log("! error server returned non-JSON response")
            raise ApiClientError(f"Server returned non-JSON response: {raw}") from exc

        if response.status >= 400:
            raise ApiClientError(f"HTTP {response.status}: {parsed}")

        return parsed

    def health(self) -> Dict[str, Any]:
        return self.request("GET", "/health")

    def get_datasets(self) -> Dict[str, Any]:
        return self.request("GET", "/api/datasets")

    def get_dataset_query_config(self, dataset_name: str) -> Dict[str, Any]:
        safe_name = quote(dataset_name, safe="")
        return self.request("GET", f"/api/datasets/{safe_name}/query-config")

    def get_filter_choices(
        self,
        dataset_name: str,
        column_name: str,
        *,
        label_search: str = "",
        bucket_count: int = 5,
    ) -> Dict[str, Any]:
        safe_dataset = quote(dataset_name, safe="")
        safe_column = quote(column_name, safe="")
        return self.request(
            "GET",
            f"/api/datasets/{safe_dataset}/filters/{safe_column}/choices",
            query={
                "label_search": label_search or None,
                "bucket_count": bucket_count,
            },
        )

    def query_rows(
        self,
        dataset_name: str,
        *,
        search_text: str = "",
        filters: Optional[List[Dict[str, Any]]] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> Dict[str, Any]:
        safe_name = quote(dataset_name, safe="")
        return self.request(
            "POST",
            f"/api/datasets/{safe_name}/rows/query",
            body={
                "search_text": search_text,
                "filters": filters or [],
                "limit": limit,
                "offset": offset,
            },
        )

    def get_row_detail(
        self,
        dataset_name: str,
        row_id: str,
        *,
        query_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        safe_dataset = quote(dataset_name, safe="")
        safe_row_id = quote(str(row_id), safe="")
        return self.request(
            "GET",
            f"/api/datasets/{safe_dataset}/rows/{safe_row_id}",
            query={"query_token": query_token},
        )

    def create_row(self, dataset_name: str, values: Dict[str, Any]) -> Dict[str, Any]:
        safe_dataset = quote(dataset_name, safe="")
        return self.request("POST", f"/api/datasets/{safe_dataset}/rows/create", body={"row": values})

    def update_row(self, dataset_name: str, row_id: str, values: Dict[str, Any]) -> Dict[str, Any]:
        safe_dataset = quote(dataset_name, safe="")
        safe_row_id = quote(str(row_id), safe="")
        return self.request(
            "PUT",
            f"/api/datasets/{safe_dataset}/rows/{safe_row_id}/update",
            body={"row": values},
        )

    def delete_row(self, dataset_name: str, row_id: str) -> Dict[str, Any]:
        safe_dataset = quote(dataset_name, safe="")
        safe_row_id = quote(str(row_id), safe="")
        return self.request("DELETE", f"/api/datasets/{safe_dataset}/rows/{safe_row_id}/delete")

    def get_output_logs(self) -> Dict[str, Any]:
        return self.request("GET", "/api/output-logs")


class ClientController:
    def __init__(self, api_client: ApiHttpClient) -> None:
        self.api = api_client
        self.current_dataset: Optional[str] = None
        self.dataset_catalog: List[Dict[str, Any]] = []
        self.search_sessions: Dict[str, SearchSessionState] = {}

    def connect(self) -> Dict[str, Any]:
        return self.api.health()

    def get_http_logs(self) -> List[str]:
        return self.api.get_logs()

    def get_output_logs(self) -> List[str]:
        try:
            response = self.api.get_output_logs()
            return response.get("data", {}).get("logs", [])
        except Exception as exc:
            return [f"<failed to load output log: {exc}>"]

    def get_datasets(self, refresh: bool = False) -> List[Dict[str, Any]]:
        if refresh or not self.dataset_catalog:
            response = self.api.get_datasets()
            self.dataset_catalog = response.get("data", {}).get("datasets", [])
        return self.dataset_catalog

    def set_dataset(self, dataset_name: str) -> None:
        self.current_dataset = dataset_name
        if dataset_name not in self.search_sessions:
            self.search_sessions[dataset_name] = SearchSessionState(dataset_name=dataset_name)

    def get_dataset(self) -> str:
        if not self.current_dataset:
            raise ApiClientError("No dataset selected.")
        return self.current_dataset

    def get_search_session(self, dataset_name: Optional[str] = None) -> SearchSessionState:
        ds = dataset_name or self.get_dataset()
        if ds not in self.search_sessions:
            self.search_sessions[ds] = SearchSessionState(dataset_name=ds)
        return self.search_sessions[ds]

    def load_query_config(self, dataset_name: Optional[str] = None, *, refresh: bool = False) -> Dict[str, Any]:
        ds = dataset_name or self.get_dataset()
        session = self.get_search_session(ds)
        if refresh or session.query_config is None:
            response = self.api.get_dataset_query_config(ds)
            session.query_config = response.get("data", {})
        return {"ok": True, "data": session.query_config or {}}

    def get_filter_choices(
        self,
        column_name: str,
        *,
        label_search: str = "",
        bucket_count: int = 5,
        dataset_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        ds = dataset_name or self.get_dataset()
        session = self.get_search_session(ds)
        cfg = session.query_config or {}
        preload = (cfg.get("prefetched_filter_choices") or {}).get(column_name)
        if preload and not label_search and int(bucket_count) == int(preload.get("bucket_count", bucket_count)):
            return {"ok": True, "data": preload}
        return self.api.get_filter_choices(ds, column_name, label_search=label_search, bucket_count=bucket_count)

    def update_search_text(self, text: str, dataset_name: Optional[str] = None) -> None:
        session = self.get_search_session(dataset_name)
        session.search_text = text
        self._clear_result_state(session)

    def set_filters(self, filters: List[Dict[str, Any]], dataset_name: Optional[str] = None) -> None:
        session = self.get_search_session(dataset_name)
        session.filters = list(filters)
        self._clear_result_state(session)

    def add_or_replace_filter(self, filter_obj: Dict[str, Any], dataset_name: Optional[str] = None) -> None:
        session = self.get_search_session(dataset_name)
        remaining = [f for f in session.filters if f.get("column") != filter_obj.get("column")]
        remaining.append(filter_obj)
        session.filters = remaining
        self._clear_result_state(session)

    def clear_search_and_filters(self, dataset_name: Optional[str] = None) -> None:
        session = self.get_search_session(dataset_name)
        session.search_text = ""
        session.filters = []
        self._clear_result_state(session)

    def run_query(self, dataset_name: Optional[str] = None, *, limit: int = 20, offset: int = 0) -> Dict[str, Any]:
        session = self.get_search_session(dataset_name)
        response = self.api.query_rows(
            session.dataset_name,
            search_text=session.search_text,
            filters=session.filters,
            limit=limit,
            offset=offset,
        )
        session.candidate_response = response
        session.query_token = response.get("data", {}).get("query_token")
        return response

    def get_selected_rows(self, dataset_name: Optional[str] = None) -> List[Dict[str, Any]]:
        session = self.get_search_session(dataset_name)
        return (session.candidate_response or {}).get("data", {}).get("rows", [])

    def get_row_detail(self, row_id: str, dataset_name: Optional[str] = None) -> Dict[str, Any]:
        session = self.get_search_session(dataset_name)
        return self.api.get_row_detail(session.dataset_name, row_id, query_token=session.query_token)

    def create_row(self, values: Dict[str, Any], dataset_name: Optional[str] = None) -> Dict[str, Any]:
        ds = dataset_name or self.get_dataset()
        response = self.api.create_row(ds, values)
        self.load_query_config(ds, refresh=True)
        return response

    def update_row(self, row_id: str, values: Dict[str, Any], dataset_name: Optional[str] = None) -> Dict[str, Any]:
        ds = dataset_name or self.get_dataset()
        response = self.api.update_row(ds, row_id, values)
        session = self.get_search_session(ds)
        self._clear_result_state(session)
        return response

    def delete_row(self, row_id: str, dataset_name: Optional[str] = None) -> Dict[str, Any]:
        ds = dataset_name or self.get_dataset()
        response = self.api.delete_row(ds, row_id)
        session = self.get_search_session(ds)
        self._clear_result_state(session)
        return response

    def _clear_result_state(self, session: SearchSessionState) -> None:
        session.candidate_response = None
        session.query_token = None


def load_ui_schema() -> Dict[str, Any]:
    schema_path = Path(__file__).resolve().parent / "new_ui_schema.json"
    with schema_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def main(host: str, port: int) -> None:
    api = ApiHttpClient(host=host, port=port, timeout=None)
    controller = ClientController(api)
    ui_schema = load_ui_schema()
    ui_main(controller, ui_schema)
