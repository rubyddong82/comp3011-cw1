import curses
import random
import textwrap
from dataclasses import dataclass
from typing import List


@dataclass
class Row:
    row_id: int
    title: str
    year: int
    rating: float
    votes: int
    status: str


class DemoCursesApp:
    def __init__(self, stdscr):
        self.stdscr = stdscr
        self.menu_items = [
            "Dashboard",
            "Datasets",
            "Row Browser",
            "Evaluation",
            "Schema",
            "Settings",
        ]
        self.menu_index = 2
        self.table_index = 0
        self.focus = "table"  # menu | table | search
        self.search_text = "batman"
        self.status_message = "Demo loaded. Nothing here talks to your real client."
        self.logs = [
            "GET /api/datasets -> 200 OK",
            "GET /api/columns?dataset=movies -> 200 OK",
            "POST /api/rows/search -> 200 OK",
        ]
        self.rows = self._make_fake_rows()
        self.filtered_rows = self._filter_rows()
        self.show_help = False
        self.show_popup = False

    def _make_fake_rows(self) -> List[Row]:
        titles = [
            "The Dark Query",
            "Return of the Cursor",
            "Curses and Columns",
            "Escape from Backspace",
            "The Schema Ultimatum",
            "Rows of Fury",
            "The Filter Identity",
            "Terminal Drift",
            "Dataset Runner 2049",
            "No Country for Null Values",
            "A Fistful of Records",
            "The Good, the Bad, and the Nullable",
        ]
        out = []
        for i, title in enumerate(titles, start=1):
            out.append(
                Row(
                    row_id=1000 + i,
                    title=title,
                    year=1980 + (i * 3) % 44,
                    rating=round(5.5 + (i * 0.31) % 4.2, 1),
                    votes=1500 + i * 731,
                    status=random.choice(["ok", "warn", "new"]),
                )
            )
        return out

    def _filter_rows(self) -> List[Row]:
        q = self.search_text.strip().lower()
        if not q:
            return self.rows[:]
        return [r for r in self.rows if q in r.title.lower()]

    def run(self):
        curses.curs_set(0)
        curses.noecho()
        curses.cbreak()
        self.stdscr.keypad(True)

        if curses.has_colors():
            curses.start_color()
            curses.use_default_colors()
            curses.init_pair(1, curses.COLOR_BLACK, curses.COLOR_CYAN)   # selected
            curses.init_pair(2, curses.COLOR_CYAN, -1)                   # accent
            curses.init_pair(3, curses.COLOR_GREEN, -1)                  # success
            curses.init_pair(4, curses.COLOR_YELLOW, -1)                 # warn
            curses.init_pair(5, curses.COLOR_MAGENTA, -1)                # popup

        while True:
            self.draw()
            ch = self.stdscr.getch()

            if self.show_help:
                self.show_help = False
                continue

            if self.show_popup:
                if ch in (27, ord("q"), ord("x"), 10, 13):
                    self.show_popup = False
                continue

            if ch in (ord("q"), ord("Q")):
                break
            elif ch == 9:  # TAB
                self.cycle_focus()
            elif ch == ord("/"):
                self.focus = "search"
                curses.curs_set(1)
            elif ch in (ord("?"), ord("h")):
                self.show_help = True
            elif ch == ord("p"):
                self.show_popup = True
            elif self.focus == "menu":
                self.handle_menu_keys(ch)
            elif self.focus == "table":
                self.handle_table_keys(ch)
            elif self.focus == "search":
                self.handle_search_keys(ch)

    def cycle_focus(self):
        order = ["menu", "table", "search"]
        idx = order.index(self.focus)
        self.focus = order[(idx + 1) % len(order)]
        curses.curs_set(1 if self.focus == "search" else 0)

    def handle_menu_keys(self, ch):
        if ch == curses.KEY_UP:
            self.menu_index = (self.menu_index - 1) % len(self.menu_items)
            self.status_message = f"Switched section to {self.menu_items[self.menu_index]}"
        elif ch == curses.KEY_DOWN:
            self.menu_index = (self.menu_index + 1) % len(self.menu_items)
            self.status_message = f"Switched section to {self.menu_items[self.menu_index]}"

    def handle_table_keys(self, ch):
        if not self.filtered_rows:
            return
        if ch == curses.KEY_UP:
            self.table_index = max(0, self.table_index - 1)
        elif ch == curses.KEY_DOWN:
            self.table_index = min(len(self.filtered_rows) - 1, self.table_index + 1)
        elif ch in (10, 13):
            row = self.filtered_rows[self.table_index]
            self.logs.insert(0, f"OPEN row_id={row.row_id} title={row.title}")
            self.logs = self.logs[:8]
            self.status_message = f"Loaded imaginary detail for row {row.row_id}"
            self.show_popup = True
        elif ch == ord("e"):
            row = self.filtered_rows[self.table_index]
            score = round(0.45 + (row.rating / 10.0) * 0.48 + min(row.votes / 20000, 0.07), 3)
            self.logs.insert(0, f"EVAL row_id={row.row_id} score={score}")
            self.logs = self.logs[:8]
            self.status_message = f"Evaluation cached for row {row.row_id}: {score}"
        elif ch == ord("r"):
            random.shuffle(self.rows)
            self.filtered_rows = self._filter_rows()
            self.table_index = 0
            self.status_message = "Refreshed fake data ordering."

    def handle_search_keys(self, ch):
        if ch in (27,):
            self.focus = "table"
            curses.curs_set(0)
            return
        if ch in (10, 13):
            self.filtered_rows = self._filter_rows()
            self.table_index = 0
            self.focus = "table"
            curses.curs_set(0)
            self.status_message = f"Search applied. {len(self.filtered_rows)} row(s) matched."
            return
        if ch in (curses.KEY_BACKSPACE, 127, 8):
            self.search_text = self.search_text[:-1]
            return
        if 32 <= ch <= 126:
            self.search_text += chr(ch)

    def draw(self):
        self.stdscr.erase()
        h, w = self.stdscr.getmaxyx()

        min_h, min_w = 24, 90
        if h < min_h or w < min_w:
            msg = f"Terminal too small. Need at least {min_w}x{min_h}, got {w}x{h}."
            self.stdscr.addstr(0, 0, msg[: w - 1])
            self.stdscr.refresh()
            return

        self.draw_header(w)
        self.draw_sidebar(h, w)
        self.draw_main_panel(h, w)
        self.draw_right_panel(h, w)
        self.draw_footer(h, w)

        if self.show_help:
            self.draw_help_popup(h, w)
        elif self.show_popup:
            self.draw_detail_popup(h, w)

        self.stdscr.refresh()

    def draw_header(self, w):
        title = " Imaginary IMDb-ish Admin Demo  curses prototype "
        self.stdscr.addstr(0, 0, " " * (w - 1), curses.color_pair(2) | curses.A_BOLD)
        self.stdscr.addstr(0, 2, title[: w - 5], curses.color_pair(2) | curses.A_BOLD)
        self.stdscr.addstr(1, 2, "Fresh demo only — not using your client/controller/schema wiring.")

    def draw_sidebar(self, h, w):
        side_w = 24
        for y in range(2, h - 2):
            self.safe_addstr(y, side_w, "│")

        self.safe_addstr(3, 2, "Sections", curses.A_BOLD)
        for i, item in enumerate(self.menu_items):
            y = 5 + i
            attr = 0
            if self.focus == "menu" and i == self.menu_index:
                attr = curses.color_pair(1) | curses.A_BOLD
            elif i == self.menu_index:
                attr = curses.A_BOLD
            label = f" {'►' if i == self.menu_index else ' '} {item}"
            self.safe_addstr(y, 2, label.ljust(side_w - 3), attr)

        self.safe_addstr(13, 2, "Quick Stats", curses.A_BOLD)
        stats = [
            f"Rows: {len(self.rows)}",
            f"Shown: {len(self.filtered_rows)}",
            "Dataset: movies",
            "Mode: demo",
        ]
        for i, line in enumerate(stats):
            self.safe_addstr(15 + i, 2, line)

    def draw_main_panel(self, h, w):
        left = 26
        main_w = w - 54
        self.safe_addstr(3, left, "Row Browser", curses.A_BOLD)
        self.safe_addstr(4, left, "Search", curses.A_BOLD)

        search_attr = curses.color_pair(1) if self.focus == "search" else 0
        search_box = f" {self.search_text}"
        self.safe_addstr(4, left + 9, search_box.ljust(max(10, main_w - 12)), search_attr)

        headers = ["ID", "Title", "Year", "Rate", "Votes", "St"]
        col_x = [left, left + 8, left + 42, left + 49, left + 56, left + 66]
        for x, head in zip(col_x, headers):
            self.safe_addstr(6, x, head, curses.A_BOLD)
        self.safe_addstr(7, left, "─" * max(10, main_w - 2))

        visible_h = h - 12
        start = 0
        if self.table_index >= visible_h:
            start = self.table_index - visible_h + 1

        rows = self.filtered_rows[start : start + visible_h]
        for i, row in enumerate(rows):
            y = 8 + i
            actual_index = start + i
            attr = 0
            if self.focus == "table" and actual_index == self.table_index:
                attr = curses.color_pair(1) | curses.A_BOLD
            elif actual_index == self.table_index:
                attr = curses.A_REVERSE

            self.safe_addstr(y, col_x[0], str(row.row_id).ljust(6), attr)
            self.safe_addstr(y, col_x[1], row.title[:32].ljust(32), attr)
            self.safe_addstr(y, col_x[2], str(row.year).ljust(4), attr)
            self.safe_addstr(y, col_x[3], f"{row.rating:.1f}".ljust(4), attr)
            self.safe_addstr(y, col_x[4], str(row.votes).ljust(8), attr)
            self.safe_addstr(y, col_x[5], row.status[:4].ljust(4), attr)

        if not self.filtered_rows:
            self.safe_addstr(9, left, "No rows matched the search.", curses.color_pair(4))

    def draw_right_panel(self, h, w):
        x = w - 27
        for y in range(2, h - 2):
            self.safe_addstr(y, x - 2, "│")

        self.safe_addstr(3, x, "Selected Row", curses.A_BOLD)
        if self.filtered_rows:
            row = self.filtered_rows[self.table_index]
            detail = [
                f"row_id: {row.row_id}",
                f"title: {row.title}",
                f"year: {row.year}",
                f"rating: {row.rating}",
                f"votes: {row.votes}",
                f"status: {row.status}",
                "",
                "Fake filters:",
                "- type = movie",
                "- rating >= 7.0",
                "- votes >= 3000",
            ]
        else:
            detail = ["No row selected."]

        for i, line in enumerate(detail):
            self.safe_addstr(5 + i, x, line[:25])

        self.safe_addstr(18, x, "Activity", curses.A_BOLD)
        for i, line in enumerate(self.logs[:7]):
            self.safe_addstr(20 + i, x, line[:25])

    def draw_footer(self, h, w):
        footer = "TAB focus  / search  ENTER open  e evaluate  r refresh  p popup  h/? help  q quit"
        self.safe_addstr(h - 2, 0, "─" * (w - 1))
        self.safe_addstr(h - 1, 1, footer[: w - 2])
        status = f"Status: {self.status_message}"
        self.safe_addstr(h - 3, 1, status[: w - 2], curses.color_pair(3))

    def draw_help_popup(self, h, w):
        popup_h, popup_w = 12, 56
        y = (h - popup_h) // 2
        x = (w - popup_w) // 2
        self.draw_box(y, x, popup_h, popup_w, "Help")
        lines = [
            "This is just a demo of how curses can feel.",
            "",
            "menu focus   : up/down to switch section",
            "table focus  : up/down to move, enter to open",
            "search focus : type text, enter to apply, esc to cancel",
            "",
            "Press any key to close this help.",
        ]
        for i, line in enumerate(lines):
            self.safe_addstr(y + 2 + i, x + 2, line[: popup_w - 4])

    def draw_detail_popup(self, h, w):
        popup_h, popup_w = 14, 60
        y = (h - popup_h) // 2
        x = (w - popup_w) // 2
        self.draw_box(y, x, popup_h, popup_w, "Imaginary Row Detail")

        if self.filtered_rows:
            row = self.filtered_rows[self.table_index]
            score = round(0.45 + (row.rating / 10.0) * 0.48 + min(row.votes / 20000, 0.07), 3)
            lines = [
                f"row_id           : {row.row_id}",
                f"title            : {row.title}",
                f"evaluation score : {score}",
                "",
                "Notes:",
                "- This popup is fake detail storage.",
                "- Good for showing how a row overlay might feel.",
                "- In your real app this could show global stat comparison,",
                "  explanation lines, and actions.",
                "",
                "Press Enter, Esc, q, or x to close.",
            ]
        else:
            lines = ["No row available."]

        yy = y + 2
        for line in lines:
            wrapped = textwrap.wrap(line, popup_w - 4) or [""]
            for part in wrapped:
                self.safe_addstr(yy, x + 2, part)
                yy += 1
                if yy >= y + popup_h - 1:
                    return

    def draw_box(self, y, x, h, w, title=""):
        try:
            self.stdscr.attron(curses.color_pair(5))
        except curses.error:
            pass
        self.safe_addstr(y, x, "┌" + "─" * (w - 2) + "┐")
        for yy in range(y + 1, y + h - 1):
            self.safe_addstr(yy, x, "│" + " " * (w - 2) + "│")
        self.safe_addstr(y + h - 1, x, "└" + "─" * (w - 2) + "┘")
        if title:
            self.safe_addstr(y, x + 2, f" {title} ", curses.A_BOLD)
        try:
            self.stdscr.attroff(curses.color_pair(5))
        except curses.error:
            pass

    def safe_addstr(self, y, x, text, attr=0):
        h, w = self.stdscr.getmaxyx()
        if y < 0 or y >= h or x >= w:
            return
        available = max(0, w - x - 1)
        if available <= 0:
            return
        try:
            self.stdscr.addstr(y, x, text[:available], attr)
        except curses.error:
            pass


def main(stdscr):
    app = DemoCursesApp(stdscr)
    app.run()


if __name__ == "__main__":
    curses.wrapper(main)
