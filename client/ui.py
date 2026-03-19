import curses
import json
import textwrap
from typing import Any, Dict, List, Optional

from client_main import (
    ApiClientError,
    ApiHttpClient,
    ClientConfig,
    ClientController,
    load_ui_schema,
)


class TerminalUi:
    def __init__(self, stdscr: Any, controller: ClientController, ui_schema: Dict[str, Any]) -> None:
        self.stdscr = stdscr
        self.controller = controller
        self.root_schema = ui_schema
        self.stack: List[Dict[str, Any]] = [self.root_schema]
        self.selected_indices: List[int] = [0]
        self.message: str = ""
        self.root_exit = False

    def run(self) -> None:
        curses.curs_set(0)
        self.stdscr.keypad(True)

        try:
            health = self.controller.connect()
            self.message = f"Connected: {self._safe_one_line(health)}"
        except ApiClientError as exc:
            self.message = f"Server connection failed: {exc}"

        while not self.root_exit:
            node = self.current_node()
            node_type = node.get("type", "menu")

            if node_type in {"menu", "dataset"}:
                self.render_menu_screen(node)
                self.handle_menu_input(node)
            elif node_type == "read_update_delete":
                self.run_read_update_delete_screen(node)
            elif node_type == "placeholder":
                self.run_placeholder_screen(node)
            else:
                self.message = f"Unsupported node type: {node_type}"
                self.go_back()

    def current_node(self) -> Dict[str, Any]:
        return self.stack[-1]

    def current_index(self) -> int:
        return self.selected_indices[-1]

    def set_current_index(self, value: int) -> None:
        self.selected_indices[-1] = value

    def push_node(self, node: Dict[str, Any]) -> None:
        self.stack.append(node)
        self.selected_indices.append(0)

    def go_back(self) -> None:
        if len(self.stack) > 1:
            self.stack.pop()
            self.selected_indices.pop()
        else:
            self.root_exit = True

    def get_breadcrumb(self) -> str:
        return " > ".join(node.get("label", "Untitled") for node in self.stack)

    def get_active_dataset_name(self) -> Optional[str]:
        for node in reversed(self.stack):
            if node.get("type") == "dataset" and node.get("dataset_name"):
                return node["dataset_name"]
        return None

    def clear(self) -> None:
        self.stdscr.erase()

    def refresh(self) -> None:
        self.stdscr.refresh()

    def dims(self) -> tuple[int, int]:
        return self.stdscr.getmaxyx()

    def write(self, y: int, x: int, text: str, attr: int = 0) -> None:
        max_y, max_x = self.dims()
        if 0 <= y < max_y:
            self.stdscr.addstr(y, x, text[: max_x - x - 1], attr)

    def draw_header(self, title: str, subtitle: Optional[str] = None) -> int:
        self.clear()
        self.write(0, 0, title, curses.A_BOLD)
        self.write(1, 0, self.get_breadcrumb())
        row = 3
        if subtitle:
            for line in self.wrap(subtitle, self.dims()[1] - 1):
                self.write(row, 0, line)
                row += 1
        return row

    def draw_footer(self) -> None:
        max_y, _ = self.dims()
        footer = "Enter=select  ↑/↓=move  x/backspace=back/exit"
        if self.message:
            footer = f"{footer} | {self.message}"
        self.write(max_y - 1, 0, footer)

    def wrap(self, text: str, width: int) -> List[str]:
        if not text:
            return [""]
        out: List[str] = []
        for part in text.splitlines() or [""]:
            out.extend(textwrap.wrap(part, max(10, width)) or [""])
        return out

    def _safe_one_line(self, data: Any) -> str:
        raw = json.dumps(data, ensure_ascii=False)
        return raw[:150]

    def render_menu_screen(self, node: Dict[str, Any]) -> None:
        subtitle = None
        if node.get("type") == "dataset":
            subtitle = f"Dataset: {node.get('dataset_name')}"

        row = self.draw_header(node.get("label", "Menu"), subtitle)
        children = node.get("children", [])
        selected = self.current_index()

        for i, child in enumerate(children):
            attr = curses.A_REVERSE if i == selected else 0
            prefix = "► " if i == selected else "  "
            self.write(row + i, 0, prefix + child.get("label", "Untitled"), attr)

        self.draw_footer()
        self.refresh()

    def handle_menu_input(self, node: Dict[str, Any]) -> None:
        children = node.get("children", [])
        if not children:
            ch = self.stdscr.getch()
            if ch in (ord("x"), ord("X"), curses.KEY_BACKSPACE, 127, 8):
                self.go_back()
            return

        ch = self.stdscr.getch()

        if ch == curses.KEY_UP:
            self.set_current_index((self.current_index() - 1) % len(children))
        elif ch == curses.KEY_DOWN:
            self.set_current_index((self.current_index() + 1) % len(children))
        elif ch in (10, 13, curses.KEY_ENTER):
            selected = children[self.current_index()]
            if selected.get("type") == "dataset" and selected.get("dataset_name"):
                self.controller.set_dataset(selected["dataset_name"])
            self.push_node(selected)
        elif ch in (ord("x"), ord("X"), curses.KEY_BACKSPACE, 127, 8):
            self.go_back()

    def run_placeholder_screen(self, node: Dict[str, Any]) -> None:
        lines = [
            f"Screen: {node.get('label', 'Placeholder')}",
            "",
            "This feature is not wired yet.",
            "",
            "API metadata:",
            json.dumps(node.get("api", {}), indent=2, ensure_ascii=False),
            "",
            "Press x or Backspace to return."
        ]
        self.run_text_screen(node.get("label", "Placeholder"), lines)

    def run_text_screen(self, title: str, lines: List[str]) -> None:
        scroll = 0
        while True:
            self.clear()
            max_y, max_x = self.dims()
            self.write(0, 0, title, curses.A_BOLD)
            self.write(1, 0, self.get_breadcrumb())

            flat: List[str] = []
            for line in lines:
                flat.extend(self.wrap(line, max_x - 1))

            body_height = max_y - 4
            visible = flat[scroll: scroll + body_height]

            row = 3
            for line in visible:
                self.write(row, 0, line)
                row += 1

            self.write(max_y - 1, 0, "↑/↓=scroll  x/backspace=return")
            self.refresh()

            ch = self.stdscr.getch()
            if ch == curses.KEY_UP:
                scroll = max(0, scroll - 1)
            elif ch == curses.KEY_DOWN:
                scroll = min(max(0, len(flat) - body_height), scroll + 1)
            elif ch in (ord("x"), ord("X"), curses.KEY_BACKSPACE, 127, 8):
                self.go_back()
                return

    def run_read_update_delete_screen(self, node: Dict[str, Any]) -> None:
        dataset_name = self.get_active_dataset_name()
        if not dataset_name:
            self.message = "No dataset selected."
            self.go_back()
            return

        self.controller.set_dataset(dataset_name)

        try:
            response = self.controller.load_query_screen_info(dataset_name)
        except ApiClientError as exc:
            self.run_text_screen(
                "Query Screen Error",
                [
                    f"Failed to load initial column detail for dataset '{dataset_name}'.",
                    "",
                    str(exc),
                ],
            )
            return

        while True:
            session = self.controller.get_search_session(dataset_name)
            data = response.get("data", {})
            filter_columns = data.get("filter_columns", [])
            candidates = []
            if session.candidate_response:
                candidates = session.candidate_response.get("data", {}).get("rows", [])

            self.render_rud_screen(
                dataset_name=dataset_name,
                query_screen_data=data,
                search_text=session.search_text,
                filters=session.filters,
                candidates=candidates,
            )

            ch = self.stdscr.getch()

            if ch in (ord("x"), ord("X"), curses.KEY_BACKSPACE, 127, 8):
                self.go_back()
                return
            elif ch == ord("s"):
                entered = self.prompt_line("Search text: ", session.search_text)
                if entered is not None:
                    self.controller.update_search_text(entered, dataset_name)
                    self.message = "Search text updated."
            elif ch == ord("c"):
                self.controller.clear_filters(dataset_name)
                self.message = "Filters cleared."
            elif ch == ord("f"):
                try:
                    new_filter = self.build_filter(dataset_name, filter_columns)
                    if new_filter is not None:
                        self.controller.add_filter(new_filter, dataset_name)
                        self.message = f"Filter added on {new_filter.get('column')}."
                except (ApiClientError, ValueError) as exc:
                    self.message = f"Filter error: {exc}"
            elif ch == ord("r"):
                try:
                    result = self.controller.run_candidate_query(dataset_name=dataset_name)
                    row_count = result.get("data", {}).get("row_count", 0)
                    self.message = f"Loaded {row_count} row candidate(s)."
                except ApiClientError as exc:
                    self.message = f"Query failed: {exc}"
            elif ch == ord("v"):
                if candidates:
                    self.run_text_screen(
                        "Candidate Rows",
                        [json.dumps(c, indent=2, ensure_ascii=False) for c in candidates],
                    )
                    return

    def render_rud_screen(
        self,
        *,
        dataset_name: str,
        query_screen_data: Dict[str, Any],
        search_text: str,
        filters: List[Dict[str, Any]],
        candidates: List[Dict[str, Any]],
    ) -> None:
        row = self.draw_header(
            "Read / Update / Delete",
            f"Dataset: {dataset_name}\n"
            "s=edit search  f=add filter  c=clear filters  r=run query  v=view results  x/backspace=back"
        )

        search_column = query_screen_data.get("search_column")
        pk_column = query_screen_data.get("primary_key_column")
        filter_columns = query_screen_data.get("filter_columns", [])

        self.write(row, 0, f"Search column: {search_column}")
        row += 1
        self.write(row, 0, f"Primary key column: {pk_column}")
        row += 1
        self.write(row, 0, f"Search text: {search_text or '(empty)'}")
        row += 2

        self.write(row, 0, "Filterable columns:", curses.A_BOLD)
        row += 1
        for col in filter_columns[:5]:
            if isinstance(col, dict):
                label = f"- {col.get('name')} [{col.get('value_kind')}]"
            else:
                label = f"- {col}"
            self.write(row, 0, label)
            row += 1

        row += 1
        self.write(row, 0, f"Active filters: {len(filters)}", curses.A_BOLD)
        row += 1
        for flt in filters[:5]:
            self.write(row, 0, "- " + json.dumps(flt, ensure_ascii=False))
            row += 1

        row += 1
        self.write(row, 0, f"Loaded candidates: {len(candidates)}", curses.A_BOLD)
        row += 1
        preview_limit = max(0, self.dims()[0] - row - 2)
        for cand in candidates[:preview_limit]:
            line = self.render_candidate_line(cand, search_column, pk_column)
            self.write(row, 0, line)
            row += 1

        self.draw_footer()
        self.refresh()

    def render_candidate_line(
        self,
        candidate: Dict[str, Any],
        search_column: Optional[str],
        pk_column: Optional[str],
    ) -> str:
        pk_val = candidate.get(pk_column) if pk_column else candidate.get("id")
        search_val = candidate.get(search_column) if search_column else ""
        score = candidate.get("evaluation_score")
        return f"- {pk_val} | {search_val} | score={score}"

    def prompt_line(self, prompt: str, initial: str = "") -> Optional[str]:
        curses.curs_set(1)
        value = initial

        while True:
            max_y, max_x = self.dims()
            self.write(max_y - 2, 0, " " * (max_x - 1))
            self.write(max_y - 2, 0, f"{prompt}{value}")
            self.stdscr.move(max_y - 2, min(len(prompt) + len(value), max_x - 2))
            self.refresh()

            ch = self.stdscr.getch()

            if ch in (10, 13, curses.KEY_ENTER):
                curses.curs_set(0)
                return value
            if ch == 27:
                curses.curs_set(0)
                return None
            if ch in (curses.KEY_BACKSPACE, 127, 8):
                value = value[:-1]
            elif 32 <= ch <= 126:
                value += chr(ch)

    def select_from_list(self, title: str, items: List[str]) -> Optional[str]:
        if not items:
            return None

        idx = 0
        while True:
            self.clear()
            self.write(0, 0, title, curses.A_BOLD)
            self.write(1, 0, "Enter=select  x/backspace=cancel")

            row = 3
            for i, item in enumerate(items):
                attr = curses.A_REVERSE if i == idx else 0
                prefix = "► " if i == idx else "  "
                self.write(row, 0, prefix + item, attr)
                row += 1

            self.refresh()

            ch = self.stdscr.getch()
            if ch == curses.KEY_UP:
                idx = (idx - 1) % len(items)
            elif ch == curses.KEY_DOWN:
                idx = (idx + 1) % len(items)
            elif ch in (10, 13, curses.KEY_ENTER):
                return items[idx]
            elif ch in (ord("x"), ord("X"), curses.KEY_BACKSPACE, 127, 8):
                return None

    def build_filter(
        self,
        dataset_name: str,
        filter_columns: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        column_names = [col["name"] for col in filter_columns if isinstance(col, dict) and "name" in col]
        column_name = self.select_from_list("Choose filter column", column_names)
        if not column_name:
            return None

        mode = self.select_from_list("Filter choice source", ["default", "search"])
        if not mode:
            return None

        if mode == "default":
            detail = self.controller.get_filter_detail_default(column_name, dataset_name=dataset_name)
        else:
            label_search = self.prompt_line("Label search: ", "")
            if label_search is None:
                return None
            detail = self.controller.get_filter_detail_from_search(
                column_name,
                label_search,
                dataset_name=dataset_name,
            )

        data = detail.get("data", {})
        choices = data.get("choices", [])
        value_kind = data.get("value_kind")

        if value_kind == "numeric":
            labels = [c.get("label", "bucket") for c in choices if isinstance(c, dict)]
            picked_label = self.select_from_list("Choose numeric bucket", labels)
            if not picked_label:
                return None

            chosen = next(
                (c for c in choices if isinstance(c, dict) and c.get("label") == picked_label),
                None,
            )
            if not chosen:
                return None

            return {
                "column": column_name,
                "kind": "numeric",
                "min": chosen.get("min"),
                "max": chosen.get("max"),
            }

        labels = []
        for c in choices:
            if isinstance(c, dict):
                label = c.get("value")
                if label is None:
                    label = c.get("label")
                if label is None:
                    label = str(c)
            else:
                label = str(c)
            labels.append(label)

        picked_value = self.select_from_list("Choose categorical value", labels)
        if not picked_value:
            return None

        return {
            "column": column_name,
            "kind": "categorical",
            "value": picked_value,
        }


def run_ui(stdscr, controller, ui_schema) -> None:
    ui = TerminalUi(stdscr, controller, ui_schema)
    ui.run()


def main(controller, ui_schema) -> None:
    curses.wrapper(lambda stdscr: run_ui(stdscr, controller, ui_schema))


if __name__ == "__main__":
    main()