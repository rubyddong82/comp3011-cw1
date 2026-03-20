#!/usr/bin/env python3
import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


def latex_escape(text: str) -> str:
    """
    Escape LaTeX special characters, but preserve common LaTeX commands
    such as \\today if user intentionally includes them.
    """
    if text is None:
        return ""

    if text.startswith("\\"):
        return text

    replacements = {
        "\\": r"\textbackslash{}",
        "&": r"\&",
        "%": r"\%",
        "$": r"\$",
        "#": r"\#",
        "_": r"\_",
        "{": r"\{",
        "}": r"\}",
        "~": r"\textasciitilde{}",
        "^": r"\textasciicircum{}",
    }

    out = []
    for ch in str(text):
        out.append(replacements.get(ch, ch))
    return "".join(out)


def build_latex(data: dict) -> str:
    title = latex_escape(data.get("title", "Technical Report"))
    author = latex_escape(data.get("author", ""))
    module = latex_escape(data.get("module", ""))
    date = latex_escape(data.get("date", r"\today"))

    links = data.get("links", {})
    github = links.get("github_repository", "")
    api_docs = links.get("api_documentation", "")
    slides = links.get("presentation_slides", [])

    sections = data.get("sections", [])
    genai = data.get("genai", {})
    appendix = data.get("appendix", {})
    conversation_logs = appendix.get("conversation_logs", [])

    slides_tex = ""
    if slides:
        slides_tex += "\\begin{itemize}\n"
        for link in slides:
            safe_link = str(link).replace("\\", "/")
            slides_tex += f"  \\item \\url{{{safe_link}}}\n"
        slides_tex += "\\end{itemize}\n"
    else:
        slides_tex = "No presentation slides or visuals provided.\n"

    sections_tex = ""
    for section in sections:
        sec_title = latex_escape(section.get("title", "Untitled Section"))
        sec_content = latex_escape(section.get("content", ""))
        sections_tex += f"\\section{{{sec_title}}}\n{sec_content}\n\n"

    logs_tex = ""
    for log in conversation_logs:
        log_title = latex_escape(log.get("title", "Untitled Log"))
        log_content = latex_escape(log.get("content", ""))
        logs_tex += f"\\subsection{{{log_title}}}\n{log_content}\n\n"

    genai_declaration = latex_escape(genai.get("declaration", ""))
    genai_analysis = latex_escape(genai.get("analysis", ""))

    latex = rf"""
\documentclass[12pt,a4paper]{{article}}

\usepackage[margin=1in]{{geometry}}
\usepackage[T1]{{fontenc}}
\usepackage[utf8]{{inputenc}}
\usepackage{{lmodern}}
\usepackage{{hyperref}}
\usepackage{{longtable}}
\usepackage{{enumitem}}
\usepackage{{titlesec}}
\usepackage{{parskip}}
\usepackage{{xcolor}}
\hypersetup{{
    colorlinks=true,
    linkcolor=blue,
    urlcolor=blue,
    pdftitle={{{title}}},
    pdfauthor={{{author}}}
}}

\title{{{title}}}
\author{{{author}\\{module}}}
\date{{{date}}}

\begin{{document}}

\maketitle
\tableofcontents
\newpage

\section{{Repository and Documentation Links}}

\subsection{{Public GitHub Repository}}
\url{{{github}}}

\subsection{{API Documentation}}
\url{{{api_docs}}}

\subsection{{Presentation Slides and Visuals}}
{slides_tex}

{sections_tex}

\section{{GenAI Declaration and Analysis}}

\subsection{{Declaration}}
{genai_declaration}

\subsection{{Analysis}}
{genai_analysis}

\appendix
\section{{Conversation Logs}}
{logs_tex}

\end{{document}}
"""
    return latex.strip() + "\n"


def run_pdflatex(tex_path: Path, output_dir: Path) -> None:
    if shutil.which("pdflatex") is None:
        raise RuntimeError(
            "pdflatex was not found in PATH. Please install a LaTeX distribution "
            "such as TeX Live or MiKTeX."
        )

    cmd = [
        "pdflatex",
        "-interaction=nonstopmode",
        "-halt-on-error",
        f"-output-directory={output_dir}",
        str(tex_path),
    ]

    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )

    if result.returncode != 0:
        raise RuntimeError(
            "pdflatex compilation failed.\n\n"
            f"STDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}"
        )


def clean_temp_files(output_dir: Path, stem: str) -> None:
    extensions = [".tex", ".aux", ".log", ".out", ".toc"]
    for ext in extensions:
        p = output_dir / f"{stem}{ext}"
        if p.exists():
            p.unlink()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a technical report PDF from a JSON file using LaTeX and pdflatex."
    )
    parser.add_argument(
        "json_file",
        help="Path to the JSON input file."
    )
    parser.add_argument(
        "-o",
        "--output",
        default="technical_report.pdf",
        help="Output PDF filename (default: technical_report.pdf)"
    )

    args = parser.parse_args()

    json_path = Path(args.json_file).resolve()
    output_pdf = Path(args.output).resolve()

    if not json_path.exists():
        print(f"Error: JSON file not found: {json_path}", file=sys.stderr)
        sys.exit(1)

    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        output_dir = output_pdf.parent
        stem = output_pdf.stem

        tex_content = build_latex(data)
        tex_path = output_dir / f"{stem}.tex"

        with open(tex_path, "w", encoding="utf-8") as f:
            f.write(tex_content)

        # Run twice so TOC resolves properly
        run_pdflatex(tex_path, output_dir)
        run_pdflatex(tex_path, output_dir)

        produced_pdf = output_dir / f"{stem}.pdf"
        if not produced_pdf.exists():
            raise RuntimeError("PDF compilation appeared to succeed, but no PDF was produced.")

        clean_temp_files(output_dir, stem)

        print(str(produced_pdf))

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()