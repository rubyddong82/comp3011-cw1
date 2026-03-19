import json
import http.client
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from client.ui import main as ui_main


class ApiClientError(Exception):
    pass


@dataclass
class SearchSessionState:
    dataset_name: str
    query_screen_info: Optional[Dict[str, Any]] = None
    search_text: str = ""
    filters: List[Dict[str, Any]] = field(default_factory=list)
    candidate_response: Optional[Dict[str, Any]] = None


class ApiHttpClient:
    def __init__(self, host: str, port: int, timeout: int = 10) -> None:
        self.host = host
        self.port = port
        self.timeout = timeout
        self._conn: Optional[http.client.HTTPConnection] = None

    def connect(self) -> None:
        self.close()
        self._conn = http.client.HTTPConnection(
            host=self.host,
            port=self.port,
            timeout=self.timeout,
        )

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None

    def _ensure_conn(self) -> http.client.HTTPConnection:
        if self._conn is None:
            self.connect()
        return self._conn

    def request(
        self,
        method: str,
        path: str,
        *,
        query: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        conn = self._ensure_conn()
        final_path = path

        if query:
            from urllib.parse import urlencode
            final_path = f"{path}?{urlencode(query)}"

        headers = {"Accept": "application/json"}
        payload = None

        if body is not None:
            payload = json.dumps(body)
            headers["Content-Type"] = "application/json"

        try:
            conn.request(method.upper(), final_path, body=payload, headers=headers)
            response = conn.getresponse()
            raw = response.read().decode("utf-8")

            try:
                parsed = json.loads(raw) if raw.strip() else {}
            except json.JSONDecodeError as exc:
                raise ApiClientError(f"Server returned non-JSON response: {raw}") from exc

            if response.status >= 400:
                raise ApiClientError(f"HTTP {response.status}: {parsed}")

            return parsed
        except (ConnectionError, OSError, http.client.HTTPException) as exc:
            self.close()
            raise ApiClientError(
                f"Failed to communicate with server at {self.host}:{self.port}: {exc}"
            ) from exc

    def health(self) -> Dict[str, Any]:
        return self.request("GET", "/health")

    def get_initial_column_detail(self, dataset_name: str) -> Dict[str, Any]:
        return self.request(
            "GET",
            "/api/initial-column-detail",
            query={"dataset_name": dataset_name},
        )

    def get_filter_column_detail_default(
        self,
        dataset_name: str,
        column_name: str,
        bucket_count: int = 5,
    ) -> Dict[str, Any]:
        return self.request(
            "POST",
            "/api/filter-column-detail/default",
            body={
                "dataset_name": dataset_name,
                "column_name": column_name,
                "bucket_count": bucket_count,
            },
        )

    def get_filter_column_detail_from_search(
        self,
        dataset_name: str,
        column_name: str,
        label_search: str,
        bucket_count: int = 5,
    ) -> Dict[str, Any]:
        return self.request(
            "POST",
            "/api/filter-column-detail/search",
            body={
                "dataset_name": dataset_name,
                "column_name": column_name,
                "label_search": label_search,
                "bucket_count": bucket_count,
            },
        )

    def get_row_candidates(
        self,
        dataset_name: str,
        *,
        search_text: str = "",
        filters: Optional[List[Dict[str, Any]]] = None,
        sort_by_evaluation: bool = True,
        limit: int = 20,
        offset: int = 0,
    ) -> Dict[str, Any]:
        return self.request(
            "POST",
            "/api/row-candidates",
            body={
                "dataset_name": dataset_name,
                "search_text": search_text,
                "filters": filters or [],
                "sort_by_evaluation": sort_by_evaluation,
                "limit": limit,
                "offset": offset,
            },
        )


class ClientController:
    def __init__(self, api_client: ApiHttpClient) -> None:
        self.api = api_client
        self.current_dataset: Optional[str] = None
        self.search_sessions: Dict[str, SearchSessionState] = {}

    def connect(self) -> Dict[str, Any]:
        self.api.connect()
        return self.api.health()

    def set_dataset(self, dataset_name: str) -> None:
        self.current_dataset = dataset_name
        if dataset_name not in self.search_sessions:
            self.search_sessions[dataset_name] = SearchSessionState(dataset_name=dataset_name)

    def get_dataset(self) -> str:
        if not self.current_dataset:
            raise ApiClientError("No dataset selected.")
        return self.current_dataset

    def load_query_screen_info(self, dataset_name: Optional[str] = None) -> Dict[str, Any]:
        ds = dataset_name or self.get_dataset()
        response = self.api.get_initial_column_detail(ds)
        if ds not in self.search_sessions:
            self.search_sessions[ds] = SearchSessionState(dataset_name=ds)
        self.search_sessions[ds].query_screen_info = response
        return response

    def get_search_session(self, dataset_name: Optional[str] = None) -> SearchSessionState:
        ds = dataset_name or self.get_dataset()
        if ds not in self.search_sessions:
            self.search_sessions[ds] = SearchSessionState(dataset_name=ds)
        return self.search_sessions[ds]

    def get_filter_detail_default(self, column_name: str, dataset_name: Optional[str] = None) -> Dict[str, Any]:
        ds = dataset_name or self.get_dataset()
        return self.api.get_filter_column_detail_default(ds, column_name)

    def get_filter_detail_from_search(self, column_name: str, label_search: str, dataset_name: Optional[str] = None) -> Dict[str, Any]:
        ds = dataset_name or self.get_dataset()
        return self.api.get_filter_column_detail_from_search(ds, column_name, label_search)

    def update_search_text(self, text: str, dataset_name: Optional[str] = None) -> None:
        session = self.get_search_session(dataset_name)
        session.search_text = text

    def clear_filters(self, dataset_name: Optional[str] = None) -> None:
        session = self.get_search_session(dataset_name)
        session.filters.clear()

    def add_filter(self, filter_obj: Dict[str, Any], dataset_name: Optional[str] = None) -> None:
        session = self.get_search_session(dataset_name)
        session.filters.append(filter_obj)

    def run_candidate_query(self, dataset_name: Optional[str] = None) -> Dict[str, Any]:
        session = self.get_search_session(dataset_name)
        response = self.api.get_row_candidates(
            session.dataset_name,
            search_text=session.search_text,
            filters=session.filters,
        )
        session.candidate_response = response
        return response


def load_ui_schema() -> Dict[str, Any]:
    schema_path = Path(__file__).resolve().parent / "ui_schema.json"
    with schema_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def main(host: str, port: int) -> None:
    api = ApiHttpClient(host=host, port=port, timeout=10)
    controller = ClientController(api)
    ui_schema = load_ui_schema()
    ui_main(controller, ui_schema)