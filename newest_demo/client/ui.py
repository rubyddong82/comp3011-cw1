import curses
import json
import threading
import time
from typing import Any, Dict, List, Optional, Tuple


class RequestCancelled(Exception):
    pass


class TerminalUi:
    SPINNER_FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]

    def __init__(self, stdscr: Any, controller: Any, ui_schema: Dict[str, Any]) -> None:
        self.stdscr = stdscr
        self.controller = controller
        self.root_schema = ui_schema
        self.stack: List[Dict[str, Any]] = [{"node": ui_schema, "state": {"selected": 0}}]
        self.message = ""
        self.root_exit = False
        self.spinner_index = 0
        self.colors: Dict[str, int] = {}

    def run(self) -> None:
        curses.curs_set(0)
        self.stdscr.keypad(True)
        self.init_colors()
        self.message = "Connecting..."
        try:
            health = self.run_with_spinner("Connecting to server", self.controller.connect)
            service = health.get("data", {}).get("service", "server")
            self.message = f"Connected to {service}."
        except RequestCancelled:
            self.message = "Initial request cancelled."
        except Exception as exc:
            self.message = f"Server connection failed: {exc}"

        while not self.root_exit:
            node = self.current_node()
            node_type = node.get("type", "menu")
            if node_type in {"menu", "dataset"}:
                self.run_menu_screen(node)
            elif node_type == "help_screen":
                self.run_help_screen(node)
            elif node_type == "create_row":
                self.run_form_screen(node, mode="create")
            elif node_type == "edit_row_form":
                self.run_form_screen(node, mode="edit")
            elif node_type == "row_crud_workbench":
                self.run_workbench_screen(node)
            elif node_type == "row_detail_editor":
                self.run_row_detail_screen(node)
            else:
                self.message = f"Unsupported screen type: {node_type}"
                self.go_back()

    def init_colors(self) -> None:
        if not curses.has_colors():
            return
        curses.start_color()
        curses.use_default_colors()
        curses.init_pair(1, curses.COLOR_CYAN, -1)
        curses.init_pair(2, curses.COLOR_BLACK, curses.COLOR_CYAN)
        curses.init_pair(3, curses.COLOR_YELLOW, -1)
        curses.init_pair(4, curses.COLOR_GREEN, -1)
        curses.init_pair(5, curses.COLOR_MAGENTA, -1)
        curses.init_pair(6, curses.COLOR_WHITE, curses.COLOR_BLUE)
        curses.init_pair(7, curses.COLOR_RED, -1)
        curses.init_pair(8, curses.COLOR_BLACK, curses.COLOR_GREEN)
        self.colors = {
            "title": curses.color_pair(1) | curses.A_BOLD,
            "active_border": curses.color_pair(2) | curses.A_BOLD,
            "panel": curses.color_pair(3),
            "ok": curses.color_pair(4) | curses.A_BOLD,
            "accent": curses.color_pair(5) | curses.A_BOLD,
            "footer": curses.color_pair(6),
            "error": curses.color_pair(7) | curses.A_BOLD,
            "button": curses.color_pair(8) | curses.A_BOLD,
        }

    def current_entry(self) -> Dict[str, Any]:
        return self.stack[-1]

    def current_node(self) -> Dict[str, Any]:
        return self.current_entry()["node"]

    def current_state(self) -> Dict[str, Any]:
        return self.current_entry()["state"]

    def push_node(self, node: Dict[str, Any], state: Optional[Dict[str, Any]] = None) -> None:
        self.stack.append({"node": node, "state": state or {}})

    def go_back(self) -> None:
        if len(self.stack) > 1:
            self.stack.pop()
        else:
            self.root_exit = True

    def dims(self) -> Tuple[int, int]:
        return self.stdscr.getmaxyx()

    def get_layout_dims(self) -> Tuple[int, int, int]:
        max_y, max_x = self.dims()
        if max_x >= 120:
            log_w = min(48, max(36, max_x // 3))
        else:
            log_w = 0
        main_w = max_x - log_w - (3 if log_w else 0)
        return max_y, main_w, log_w

    def clear(self) -> None:
        self.stdscr.erase()

    def refresh(self) -> None:
        self.stdscr.refresh()

    def write(self, y: int, x: int, text: str, attr: int = 0) -> None:
        max_y, max_x = self.dims()
        if y < 0 or y >= max_y or x >= max_x:
            return
        safe = text[: max(0, max_x - x - 1)]
        try:
            self.stdscr.addstr(y, x, safe, attr)
        except curses.error:
            pass

    def draw_box(self, y: int, x: int, h: int, w: int, title: str, active: bool = False) -> None:
        if h < 3 or w < 4:
            return
        attr = self.colors.get("active_border", curses.A_BOLD) if active else self.colors.get("panel", 0)
        self.write(y, x, "┌" + "─" * (w - 2) + "┐", attr)
        for row in range(y + 1, y + h - 1):
            self.write(row, x, "│", attr)
            self.write(row, x + w - 1, "│", attr)
        self.write(y + h - 1, x, "└" + "─" * (w - 2) + "┘", attr)
        self.write(y, x + 2, f" {title} ", attr)

    def draw_header(self, title: str, subtitle: str = "") -> int:
        _, main_w, _ = self.get_layout_dims()
        self.write(0, 2, title, self.colors.get("title", curses.A_BOLD))
        if subtitle:
            self.write(1, 2, subtitle[: max(0, main_w - 4)], self.colors.get("accent", 0))
        return 3

    def draw_footer(self, hint: str = "Enter=select  Backspace/x=back") -> None:
        max_y, max_x = self.dims()
        footer = hint
        if self.message:
            footer += f" | {self.message}"
        fill = " " * max(0, max_x - 1)
        self.write(max_y - 1, 0, fill, self.colors.get("footer", curses.A_REVERSE))
        self.write(max_y - 1, 1, footer, self.colors.get("footer", curses.A_REVERSE))

    def draw_http_log_panel(self, top_y: int = 0) -> None:
        max_y, main_w, log_w = self.get_layout_dims()
        if log_w <= 0:
            return
        x = main_w + 2
        h = max_y - top_y - 1
        self.draw_box(top_y, x, h, log_w, "HTTP Log", active=False)
        logs = self.controller.get_http_logs()
        visible_h = h - 2
        wrapped: List[str] = []
        width = max(8, log_w - 3)
        for line in logs[-60:]:
            while len(line) > width:
                wrapped.append(line[:width])
                line = line[width:]
            wrapped.append(line)
        for i, line in enumerate(wrapped[-visible_h:]):
            self.write(top_y + 1 + i, x + 1, line)

    def draw_loading_overlay(self, label: str) -> None:
        self.clear()
        self.draw_http_log_panel(0)
        max_y, main_w, _ = self.get_layout_dims()
        h, w = 7, min(56, max(24, main_w - 4))
        y = max(1, (max_y - h) // 2)
        x = max(2, (main_w - w) // 2)
        self.draw_box(y, x, h, w, "Working", active=True)
        spinner = self.SPINNER_FRAMES[self.spinner_index]
        self.write(y + 2, x + 3, f"{spinner} {label}", self.colors.get("ok", curses.A_BOLD))
        self.write(y + 4, x + 3, "Press x / Backspace / Esc to cancel waiting.", self.colors.get("panel", 0))
        self.refresh()

    def run_with_spinner(self, label: str, func: Any, *args: Any, **kwargs: Any) -> Any:
        result: Dict[str, Any] = {}
        error: Dict[str, Exception] = {}
        done = threading.Event()

        def target() -> None:
            try:
                result["value"] = func(*args, **kwargs)
            except Exception as exc:
                error["value"] = exc
            finally:
                done.set()

        thread = threading.Thread(target=target, daemon=True)
        thread.start()
        old_delay = self.stdscr.nodelay(True)
        try:
            while not done.is_set():
                self.draw_loading_overlay(label)
                ch = self.stdscr.getch()
                if ch in (ord("x"), ord("X"), curses.KEY_BACKSPACE, 127, 8, 27):
                    self.message = f"Cancelled waiting for: {label}"
                    raise RequestCancelled(label)
                time.sleep(0.08)
                self.spinner_index = (self.spinner_index + 1) % len(self.SPINNER_FRAMES)
        finally:
            self.stdscr.nodelay(old_delay)

        if "value" in error:
            raise error["value"]
        return result.get("value")

    def prompt_line(self, title: str, initial: str = "") -> Optional[str]:
        curses.curs_set(1)
        value = list(initial)
        cursor = len(value)
        while True:
            max_y, main_w, _ = self.get_layout_dims()
            box_w = max(24, main_w - 4)
            self.draw_box(max_y - 5, 2, 4, box_w, title, active=True)
            rendered = "".join(value)
            self.write(max_y - 3, 4, " " * max(1, box_w - 6))
            self.write(max_y - 3, 4, rendered)
            self.stdscr.move(max_y - 3, min(box_w - 3, 4 + cursor))
            self.draw_http_log_panel(0)
            self.refresh()
            ch = self.stdscr.getch()
            if ch in (10, 13, curses.KEY_ENTER):
                curses.curs_set(0)
                return "".join(value)
            if ch in (27,):
                curses.curs_set(0)
                return None
            if ch in (curses.KEY_BACKSPACE, 127, 8):
                if cursor > 0:
                    del value[cursor - 1]
                    cursor -= 1
            elif ch == curses.KEY_DC:
                if cursor < len(value):
                    del value[cursor]
            elif ch == curses.KEY_LEFT:
                cursor = max(0, cursor - 1)
            elif ch == curses.KEY_RIGHT:
                cursor = min(len(value), cursor + 1)
            elif ch == curses.KEY_HOME:
                cursor = 0
            elif ch == curses.KEY_END:
                cursor = len(value)
            elif ch == 21:  # Ctrl-U
                value = []
                cursor = 0
            elif 32 <= ch <= 126:
                value.insert(cursor, chr(ch))
                cursor += 1

    def confirm(self, message: str) -> bool:
        idx = 0
        options = ["No", "Yes"]
        while True:
            self.clear()
            self.draw_http_log_panel(0)
            max_y, main_w, _ = self.get_layout_dims()
            self.draw_box(max_y // 2 - 3, max(2, main_w // 2 - 24), 7, min(48, main_w - 4), "Confirm", active=True)
            self.write(max_y // 2 - 1, max(4, main_w // 2 - 20), message)
            row_y = max_y // 2 + 1
            base_x = max(6, main_w // 2 - 12)
            for i, item in enumerate(options):
                label = f"[ {item} ]"
                attr = self.colors.get("button", curses.A_REVERSE) if i == idx else 0
                self.write(row_y, base_x + i * 14, label, attr)
            self.draw_footer("←/→ move  Enter=confirm  x/backspace=cancel")
            self.refresh()
            ch = self.stdscr.getch()
            if ch in (curses.KEY_LEFT, curses.KEY_UP):
                idx = (idx - 1) % len(options)
            elif ch in (curses.KEY_RIGHT, curses.KEY_DOWN):
                idx = (idx + 1) % len(options)
            elif ch in (10, 13, curses.KEY_ENTER):
                return options[idx] == "Yes"
            elif ch in (ord("x"), ord("X"), curses.KEY_BACKSPACE, 127, 8):
                return False

    def select_from_list(self, title: str, choices: List[Tuple[str, Any]], subtitle: str = "") -> Optional[Any]:
        if not choices:
            self.message = "No choices available."
            return None
        idx = 0
        scroll = 0
        while True:
            self.clear()
            start_row = self.draw_header(title, subtitle)
            self.draw_http_log_panel(0)
            max_y, main_w, _ = self.get_layout_dims()
            box_h = max_y - start_row - 2
            self.draw_box(start_row, 2, box_h, main_w - 4, title, active=True)
            visible_h = box_h - 2
            if idx < scroll:
                scroll = idx
            elif idx >= scroll + visible_h:
                scroll = idx - visible_h + 1
            for i, (label, _) in enumerate(choices[scroll : scroll + visible_h]):
                absolute = scroll + i
                attr = self.colors.get("button", curses.A_REVERSE) if absolute == idx else 0
                self.write(start_row + 1 + i, 4, label[: max(0, main_w - 8)], attr)
            self.draw_footer("↑/↓ move  Enter=select  x/backspace=cancel")
            self.refresh()
            ch = self.stdscr.getch()
            if ch == curses.KEY_UP:
                idx = (idx - 1) % len(choices)
            elif ch == curses.KEY_DOWN:
                idx = (idx + 1) % len(choices)
            elif ch in (10, 13, curses.KEY_ENTER):
                return choices[idx][1]
            elif ch in (ord("x"), ord("X"), curses.KEY_BACKSPACE, 127, 8):
                return None

    def run_menu_screen(self, node: Dict[str, Any]) -> None:
        state = self.current_state()
        children = node.get("children", [])
        state.setdefault("selected", 0)
        if node.get("type") == "dataset":
            dataset_name = node.get("dataset_name")
            if dataset_name:
                self.controller.set_dataset(dataset_name)
                try:
                    self.run_with_spinner("Loading dataset config and filter summaries", self.controller.load_query_config, dataset_name, refresh=False)
                except RequestCancelled:
                    pass
                except Exception as exc:
                    self.message = f"Failed to load dataset config: {exc}"
        while True:
            self.clear()
            title = node.get("label", "Menu")
            subtitle = "Choose an option"
            start_row = self.draw_header(title, subtitle)
            self.draw_http_log_panel(0)
            max_y, main_w, _ = self.get_layout_dims()
            box_h = max_y - start_row - 2
            self.draw_box(start_row, 2, box_h, main_w - 4, title, active=True)
            for i, child in enumerate(children[: box_h - 2]):
                attr = self.colors.get("button", curses.A_REVERSE) if i == state["selected"] else 0
                self.write(start_row + 1 + i, 5, child.get("label", "Untitled"), attr)
            self.draw_footer()
            self.refresh()
            ch = self.stdscr.getch()
            if ch == curses.KEY_UP:
                state["selected"] = (state["selected"] - 1) % max(1, len(children))
            elif ch == curses.KEY_DOWN:
                state["selected"] = (state["selected"] + 1) % max(1, len(children))
            elif ch in (10, 13, curses.KEY_ENTER):
                if not children:
                    continue
                selected_node = children[state["selected"]]
                self.push_node(selected_node, {"selected": 0})
                return
            elif ch in (ord("x"), ord("X"), curses.KEY_BACKSPACE, 127, 8):
                self.go_back()
                return

    def run_help_screen(self, node: Dict[str, Any]) -> None:
        self.current_state().setdefault("scroll", 0)
        content = node.get("content", [])
        lines = [str(x) for x in content]
        while True:
            self.clear()
            start_row = self.draw_header(node.get("title", node.get("label", "Help")), "Read and press back to return")
            self.draw_http_log_panel(0)
            max_y, main_w, _ = self.get_layout_dims()
            box_h = max_y - start_row - 2
            self.draw_box(start_row, 2, box_h, main_w - 4, "Help", active=True)
            visible_h = box_h - 2
            scroll = self.current_state()["scroll"]
            for i, line in enumerate(lines[scroll : scroll + visible_h]):
                self.write(start_row + 1 + i, 4, line[: max(0, main_w - 8)])
            self.draw_footer("↑/↓ scroll  x/backspace=back")
            self.refresh()
            ch = self.stdscr.getch()
            if ch == curses.KEY_UP:
                self.current_state()["scroll"] = max(0, self.current_state()["scroll"] - 1)
            elif ch == curses.KEY_DOWN:
                self.current_state()["scroll"] = min(max(0, len(lines) - visible_h), self.current_state()["scroll"] + 1)
            elif ch in (ord("x"), ord("X"), curses.KEY_BACKSPACE, 127, 8):
                self.go_back()
                return

    def run_form_screen(self, node: Dict[str, Any], mode: str) -> None:
        dataset_name = self.controller.get_dataset()
        state = self.current_state()
        try:
            config = self.run_with_spinner("Loading form config", self.controller.load_query_config, dataset_name)["data"]
        except RequestCancelled:
            return
        if mode == "create":
            fields = list(config.get("create_fields", []))
            values = state.setdefault("values", {f["name"]: "" for f in fields})
            row_id = None
        else:
            row_id = state.get("row_id")
            try:
                detail = self.run_with_spinner("Loading row detail", self.controller.get_row_detail, row_id, dataset_name)
            except RequestCancelled:
                return
            row = detail.get("data", {}).get("row", {})
            fields = list(config.get("editable_fields", []))
            values = state.setdefault(
                "values",
                {f["name"]: "" if row.get(f["name"]) is None else str(row.get(f["name"])) for f in fields},
            )
        state.setdefault("selected", 0)
        state.setdefault("scroll", 0)
        while True:
            self.clear()
            title = node.get("title", "Form")
            subtitle = "Enter edits fields. Ctrl-U clears the current input line."
            start_row = self.draw_header(title, subtitle)
            self.draw_http_log_panel(0)
            max_y, main_w, _ = self.get_layout_dims()
            box_h = max_y - start_row - 2
            self.draw_box(start_row, 2, box_h, main_w - 4, title, active=True)
            visible_h = box_h - 4
            items = [("field", f["name"]) for f in fields] + [("action", "Save"), ("action", "Back")]
            selected = min(state["selected"], len(items) - 1)
            state["selected"] = selected
            scroll = state["scroll"]
            if selected < scroll:
                scroll = selected
            elif selected >= scroll + visible_h:
                scroll = selected - visible_h + 1
            state["scroll"] = scroll
            for i, (kind, key) in enumerate(items[scroll : scroll + visible_h]):
                absolute = scroll + i
                attr = self.colors.get("button", curses.A_REVERSE) if absolute == selected else 0
                if kind == "field":
                    display = str(values.get(key, ""))
                    self.write(start_row + 1 + i, 4, f"{key:16} : {display}"[: max(0, main_w - 8)], attr)
                else:
                    self.write(start_row + 1 + i, 4, f"[ {key} ]", attr)
            self.draw_footer("↑/↓ move  Enter=edit/select  x/backspace=back")
            self.refresh()
            ch = self.stdscr.getch()
            if ch == curses.KEY_UP:
                state["selected"] = (state["selected"] - 1) % len(items)
            elif ch == curses.KEY_DOWN:
                state["selected"] = (state["selected"] + 1) % len(items)
            elif ch in (10, 13, curses.KEY_ENTER):
                kind, key = items[state["selected"]]
                if kind == "field":
                    entered = self.prompt_line(f"Edit {key}", str(values.get(key, "")))
                    if entered is not None:
                        values[key] = entered
                elif key == "Save":
                    try:
                        if mode == "create":
                            result = self.run_with_spinner("Creating row", self.controller.create_row, values, dataset_name)
                            created_id = result.get("data", {}).get("row_id") or result.get("data", {}).get("row", {}).get("tconst")
                            self.message = f"Row created: {created_id}"
                        else:
                            self.run_with_spinner("Updating row", self.controller.update_row, row_id, values, dataset_name)
                            self.message = f"Row updated: {row_id}"
                        self.go_back()
                        return
                    except RequestCancelled:
                        pass
                    except Exception as exc:
                        self.message = f"Save failed: {exc}"
                elif key == "Back":
                    self.go_back()
                    return
            elif ch in (ord("x"), ord("X"), curses.KEY_BACKSPACE, 127, 8):
                self.go_back()
                return

    def run_workbench_screen(self, node: Dict[str, Any]) -> None:
        dataset_name = self.controller.get_dataset()
        state = self.current_state()
        state.setdefault("active_section", 0)
        state.setdefault("filter_index", 0)
        state.setdefault("action_index", 0)
        state.setdefault("result_index", 0)
        try:
            config = self.run_with_spinner("Loading query config", self.controller.load_query_config, dataset_name)["data"]
        except RequestCancelled:
            return
        while True:
            session = self.controller.get_search_session(dataset_name)
            filter_columns = config.get("filter_columns", [])
            results = self.controller.get_selected_rows(dataset_name)
            sections = ["search", "filters", "actions"] + (["results"] if results else [])
            active_section = sections[state["active_section"] % len(sections)]
            state["active_section"] = sections.index(active_section)

            self.clear()
            subtitle = f"Dataset: {dataset_name} | Search column: {config.get('search_column')}"
            start_row = self.draw_header(node.get("title", "Workbench"), subtitle)
            self.draw_http_log_panel(0)
            max_y, main_w, _ = self.get_layout_dims()
            body_h = max_y - start_row - 2
            top_h = 5
            mid_h = max(8, body_h // 3)
            action_h = 5
            result_h = body_h - top_h - mid_h - action_h
            if result_h < 4:
                result_h = 4
                mid_h = max(6, body_h - top_h - action_h - result_h)

            self.draw_box(start_row, 2, top_h, main_w - 4, "Search", active=(active_section == "search"))
            self.write(start_row + 1, 4, "Enter to edit. Backspace works. Ctrl-U clears during edit.", self.colors.get("panel", 0))
            self.write(start_row + 2, 4, session.search_text or "<empty>", self.colors.get("accent", 0))

            middle_y = start_row + top_h
            self.draw_box(middle_y, 2, mid_h, main_w - 4, "Columns / Filters", active=(active_section == "filters"))
            active_filter_map = {flt.get("column"): flt for flt in session.filters}
            visible_mid = mid_h - 2
            filter_scroll = state.setdefault("filter_scroll", 0)
            if state["filter_index"] < filter_scroll:
                filter_scroll = state["filter_index"]
            elif state["filter_index"] >= filter_scroll + visible_mid:
                filter_scroll = state["filter_index"] - visible_mid + 1
            state["filter_scroll"] = filter_scroll
            for i, col in enumerate(filter_columns[filter_scroll : filter_scroll + visible_mid]):
                absolute = filter_scroll + i
                selected = active_section == "filters" and absolute == state["filter_index"]
                attr = self.colors.get("button", curses.A_REVERSE) if selected else 0
                label = f"{col['name']:16} [{col['value_kind']}]"
                if col["name"] in active_filter_map:
                    label += f"  => {self._format_filter(active_filter_map[col['name']])}"
                self.write(middle_y + 1 + i, 4, label[: max(0, main_w - 8)], attr)

            action_y = middle_y + mid_h
            self.draw_box(action_y, 2, action_h, main_w - 4, "Actions", active=(active_section == "actions"))
            action_labels = ["Query", "Clear Filter/Search"]
            base_x = 5
            for i, label in enumerate(action_labels):
                attr = self.colors.get("button", curses.A_REVERSE) if active_section == "actions" and i == state["action_index"] else 0
                self.write(action_y + 2, base_x, f"[ {label} ]", attr)
                base_x += len(label) + 8

            if results:
                result_y = action_y + action_h
                self.draw_box(result_y, 2, result_h, main_w - 4, "Selected Rows", active=(active_section == "results"))
                visible_res = result_h - 2
                res_scroll = state.setdefault("result_scroll", 0)
                if state["result_index"] < res_scroll:
                    res_scroll = state["result_index"]
                elif state["result_index"] >= res_scroll + visible_res:
                    res_scroll = state["result_index"] - visible_res + 1
                state["result_scroll"] = res_scroll
                for i, row in enumerate(results[res_scroll : res_scroll + visible_res]):
                    absolute = res_scroll + i
                    selected = active_section == "results" and absolute == state["result_index"]
                    attr = self.colors.get("button", curses.A_REVERSE) if selected else 0
                    title = str(row.get("primaryTitle") or "")
                    display = f"{row.get('tconst')} | {title[:20]:20} | {row.get('startYear') or '-':>4} | rating={row.get('averageRating') or '-':>4} | votes={row.get('numVotes') or 0:>6} | eval={row.get('evaluation_score', 0):.3f}"
                    self.write(result_y + 1 + i, 4, display[: max(0, main_w - 8)], attr)

            self.draw_footer("←/→ section  ↑/↓ move  Enter=select  x/backspace=back")
            self.refresh()
            ch = self.stdscr.getch()
            if ch == curses.KEY_LEFT:
                state["active_section"] = (state["active_section"] - 1) % len(sections)
            elif ch == curses.KEY_RIGHT:
                state["active_section"] = (state["active_section"] + 1) % len(sections)
            elif ch == curses.KEY_UP:
                if active_section == "filters" and filter_columns:
                    state["filter_index"] = (state["filter_index"] - 1) % len(filter_columns)
                elif active_section == "actions":
                    state["action_index"] = (state["action_index"] - 1) % len(action_labels)
                elif active_section == "results" and results:
                    state["result_index"] = (state["result_index"] - 1) % len(results)
            elif ch == curses.KEY_DOWN:
                if active_section == "filters" and filter_columns:
                    state["filter_index"] = (state["filter_index"] + 1) % len(filter_columns)
                elif active_section == "actions":
                    state["action_index"] = (state["action_index"] + 1) % len(action_labels)
                elif active_section == "results" and results:
                    state["result_index"] = (state["result_index"] + 1) % len(results)
            elif ch in (10, 13, curses.KEY_ENTER):
                if active_section == "search":
                    entered = self.prompt_line("Search title", session.search_text)
                    if entered is not None:
                        self.controller.update_search_text(entered, dataset_name)
                        self.message = "Search updated."
                elif active_section == "filters" and filter_columns:
                    column = filter_columns[state["filter_index"]]
                    self.handle_filter_pick(dataset_name, column["name"], column["value_kind"])
                elif active_section == "actions":
                    if state["action_index"] == 0:
                        try:
                            self.run_with_spinner("Querying rows", self.controller.run_query, dataset_name)
                            self.message = f"Query complete. {len(self.controller.get_selected_rows(dataset_name))} rows loaded."
                            state["result_index"] = 0
                            state["active_section"] = len(sections)
                        except RequestCancelled:
                            pass
                        except Exception as exc:
                            self.message = f"Query failed: {exc}"
                    else:
                        self.controller.clear_search_and_filters(dataset_name)
                        self.message = "Search and filters cleared."
                        state["result_index"] = 0
                elif active_section == "results" and results:
                    selected_row = results[state["result_index"]]
                    detail_node = node.get("children", [])[0]
                    self.push_node(detail_node, {"row_id": str(selected_row.get("tconst")), "scroll": 0, "action_index": 0, "section": 0})
                    return
            elif ch in (ord("x"), ord("X"), curses.KEY_BACKSPACE, 127, 8):
                self.go_back()
                return

    def handle_filter_pick(self, dataset_name: str, column_name: str, value_kind: str) -> None:
        try:
            data = self.run_with_spinner(
                f"Loading filter options for {column_name}",
                self.controller.get_filter_choices,
                column_name,
                dataset_name=dataset_name,
            ).get("data", {})
        except RequestCancelled:
            return
        except Exception as exc:
            self.message = f"Filter request failed: {exc}"
            return

        choices = data.get("choices", [])
        if value_kind == "categorical":
            selection = self.select_from_list(
                f"Filter: {column_name}",
                [(f"{c.get('label')} ({c.get('count')})", c) for c in choices],
                "Categorical values sorted by frequency",
            )
            if selection is not None:
                self.controller.add_or_replace_filter(
                    {"column": column_name, "kind": "categorical", "value": selection.get("value")},
                    dataset_name,
                )
                self.message = f"Filter set on {column_name}."
        else:
            selection = self.select_from_list(
                f"Filter: {column_name}",
                [(f"{c.get('label')} ({c.get('count')})", c) for c in choices],
                "Numeric buckets",
            )
            if selection is not None:
                self.controller.add_or_replace_filter(
                    {
                        "column": column_name,
                        "kind": "numeric",
                        "min": selection.get("min"),
                        "max": selection.get("max"),
                    },
                    dataset_name,
                )
                self.message = f"Filter set on {column_name}."

    def run_row_detail_screen(self, node: Dict[str, Any]) -> None:
        dataset_name = self.controller.get_dataset()
        state = self.current_state()
        row_id = state.get("row_id")
        try:
            detail = self.run_with_spinner("Loading row detail", self.controller.get_row_detail, row_id, dataset_name).get("data", {})
        except RequestCancelled:
            return
        except Exception as exc:
            self.message = f"Failed to load row detail: {exc}"
            self.go_back()
            return
        row = detail.get("row", {})
        evaluation = detail.get("evaluation", {})
        while True:
            self.clear()
            start_row = self.draw_header(node.get("title", "Selected Row"), f"Row ID: {row_id}")
            self.draw_http_log_panel(0)
            max_y, main_w, _ = self.get_layout_dims()
            body_h = max_y - start_row - 2
            left_w = max(36, (main_w - 6) * 2 // 3)
            right_w = main_w - 6 - left_w
            self.draw_box(start_row, 2, body_h, left_w, "Movie Detail", active=(state.get("section", 0) == 0))
            self.draw_box(start_row, 3 + left_w, body_h, right_w, "Actions", active=(state.get("section", 0) == 1))
            lines = [
                f"{k}: {row.get(k)}"
                for k in [
                    "tconst", "primaryTitle", "originalTitle", "titleType", "isAdult", "startYear", "endYear",
                    "runtimeMinutes", "genres", "averageRating", "numVotes",
                ]
            ]
            lines += ["", "Evaluation"]
            for key in [
                "score", "summary", "average_rating", "num_votes", "global_mean_rating", "global_votes_threshold",
                "weighted_rating", "recency_norm", "recency_multiplier", "final_score",
            ]:
                if key in evaluation:
                    lines.append(f"{key}: {evaluation.get(key)}")
            scroll = state.setdefault("scroll", 0)
            visible = body_h - 2
            max_scroll = max(0, len(lines) - visible)
            scroll = min(scroll, max_scroll)
            state["scroll"] = scroll
            for i, line in enumerate(lines[scroll : scroll + visible]):
                self.write(start_row + 1 + i, 4, line[: max(0, left_w - 4)])

            actions = ["Edit", "Delete", "Back"]
            selected = state.setdefault("action_index", 0)
            for i, item in enumerate(actions):
                attr = self.colors.get("button", curses.A_REVERSE) if state.get("section", 0) == 1 and i == selected else 0
                self.write(start_row + 2 + i * 2, 5 + left_w, f"[ {item} ]", attr)

            self.draw_footer("←/→ section  ↑/↓ move  Enter=select  x/backspace=back")
            self.refresh()
            ch = self.stdscr.getch()
            if ch in (curses.KEY_LEFT, curses.KEY_RIGHT):
                state["section"] = 1 - state.get("section", 0)
            elif ch == curses.KEY_UP:
                if state.get("section", 0) == 0:
                    state["scroll"] = max(0, state["scroll"] - 1)
                else:
                    state["action_index"] = (state["action_index"] - 1) % len(actions)
            elif ch == curses.KEY_DOWN:
                if state.get("section", 0) == 0:
                    state["scroll"] = min(max_scroll, state["scroll"] + 1)
                else:
                    state["action_index"] = (state["action_index"] + 1) % len(actions)
            elif ch in (10, 13, curses.KEY_ENTER):
                if state.get("section", 0) == 1:
                    action = actions[state["action_index"]]
                    if action == "Edit":
                        edit_node = node.get("children", [])[0]
                        self.push_node(edit_node, {"row_id": row_id, "selected": 0})
                        return
                    if action == "Delete":
                        if self.confirm(f"Delete row {row_id}?"):
                            try:
                                self.run_with_spinner("Deleting row", self.controller.delete_row, row_id, dataset_name)
                                self.message = f"Row deleted: {row_id}"
                                self.go_back()
                                return
                            except RequestCancelled:
                                pass
                            except Exception as exc:
                                self.message = f"Delete failed: {exc}"
                    if action == "Back":
                        self.go_back()
                        return
            elif ch in (ord("x"), ord("X"), curses.KEY_BACKSPACE, 127, 8):
                self.go_back()
                return

    def _format_filter(self, flt: Dict[str, Any]) -> str:
        if flt.get("kind") == "categorical":
            return str(flt.get("value"))
        if flt.get("kind") == "numeric":
            return f"{flt.get('min')}..{flt.get('max')}"
        return json.dumps(flt)


def run_ui(stdscr: Any, controller: Any, ui_schema: Dict[str, Any]) -> None:
    ui = TerminalUi(stdscr, controller, ui_schema)
    ui.run()


def main(controller: Any, ui_schema: Dict[str, Any]) -> None:
    curses.wrapper(lambda stdscr: run_ui(stdscr, controller, ui_schema))
