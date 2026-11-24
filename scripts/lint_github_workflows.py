#!/usr/bin/env python3
"""Vang ongeldige GitHub-workflows en garage-jobs zonder lxml.

Een ongeldig workflowbestand op main zet het schema uit. GitHub draait dan
de vorige, nog geldige versie niet verder. ktm_new_products_daily start
niet meer, en de watchdog faalt met HTTP 422 (geen workflow_dispatch).

Draai vanaf de projectroot:

  python3 scripts/lint_github_workflows.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"

# De watchdog start de daily via de API. Zonder deze triggers valt de import stil.
REQUIRED_TRIGGERS = {
    "ktm_new_products_daily.yml": frozenset({"schedule", "workflow_dispatch"}),
    "ktm_new_products_watchdog.yml": frozenset({"schedule", "workflow_dispatch"}),
}

# python3 scripts/foo.py op de runner. \$PY op de VPS telt niet mee.
_LOCAL_SCRIPT = re.compile(
    r"(?<![\w$\\])python3?(?:\s+-u)?\s+(scripts/[\w./-]+\.py)"
)
_SCRIPT_JOIN = re.compile(r"""['"]scripts['"]\s*/\s*['"]([\w./-]+\.py)['"]""")
_SCRIPT_SLASH = re.compile(r"scripts/([\w./-]+\.py)")
_IMPORT = re.compile(
    r"^\s*(?:from\s+([A-Za-z_][\w.]*)\s+import|import\s+([A-Za-z_][\w.]*))",
    re.M,
)
_LXML = re.compile(r"^\s*(?:import\s+lxml\b|from\s+lxml\b)", re.M)


def main() -> int:
    problems = lint_repo(ROOT)
    if problems:
        print(f"{len(problems)} workflow-probleem(en):", file=sys.stderr)
        for problem in problems:
            print(f"- {problem}", file=sys.stderr)
        return 1
    print(f"workflows ok ({len(list(WORKFLOWS.glob('*.yml')))} bestanden)")
    return 0


def lint_repo(root: Path) -> list[str]:
    problems: list[str] = []
    workflow_dir = root / ".github" / "workflows"
    paths = sorted(workflow_dir.glob("*.yml"))
    if not paths:
        return [f"geen workflows in {workflow_dir}"]
    for path in paths:
        problems.extend(lint_workflow(root, path))
    return problems


def lint_workflow(root: Path, path: Path) -> list[str]:
    try:
        import yaml
    except ImportError:
        return ["PyYAML ontbreekt. Installeer met: pip install 'pyyaml>=6,<7'"]

    text = path.read_text(encoding="utf-8")
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        return [
            f"{path.name}: ongeldige YAML ({_yaml_where(exc)}). "
            "Een regel op kolom 0 binnen run: | maakt het hele bestand ongeldig; "
            "GitHub zet dan schedule en workflow_dispatch uit."
        ]
    if not isinstance(data, dict):
        return [f"{path.name}: workflow is geen mapping"]

    problems: list[str] = []
    triggers = _triggers(data)
    required = REQUIRED_TRIGGERS.get(path.name)
    if required:
        missing = required - triggers
        if missing:
            problems.append(
                f"{path.name}: mist {', '.join(sorted(missing))}. "
                "Zonder workflow_dispatch kan de watchdog de daily niet starten."
            )

    jobs = data.get("jobs") or {}
    if not isinstance(jobs, dict):
        return problems + [f"{path.name}: jobs is geen mapping"]
    for job_name, job in jobs.items():
        if not isinstance(job, dict):
            continue
        problems.extend(job_dependency_problems(root, path.name, str(job_name), job))
    return problems


def job_dependency_problems(
    root: Path, workflow_name: str, job_name: str, job: dict
) -> list[str]:
    """Een job die lokaal een script met lxml draait, moet requirements.txt installeren."""
    scripts = _local_scripts(job)
    if not scripts:
        return []
    needs: list[str] = []
    for rel in sorted(scripts):
        path = root / rel
        if not path.is_file():
            continue
        if _needs_lxml(root, path, set()):
            needs.append(rel)
    if not needs or _installs_project_deps(job):
        return []
    joined = ", ".join(needs)
    return [
        f"{workflow_name} job {job_name}: draait {joined} (heeft lxml) "
        "zonder `pip install -r requirements.txt`. Alleen requests is niet genoeg."
    ]


def _triggers(data: dict) -> set[str]:
    # PyYAML leest de sleutel `on` als True.
    raw = data.get(True, data.get("on"))
    if isinstance(raw, dict):
        return {str(key) for key in raw}
    if isinstance(raw, list):
        return {str(key) for key in raw}
    if isinstance(raw, str):
        return {raw}
    return set()


def _local_scripts(job: dict) -> set[str]:
    found: set[str] = set()
    for step in job.get("steps") or []:
        if not isinstance(step, dict):
            continue
        run = step.get("run")
        if not isinstance(run, str):
            continue
        for match in _LOCAL_SCRIPT.finditer(run):
            found.add(match.group(1))
    return found


def _installs_project_deps(job: dict) -> bool:
    for step in job.get("steps") or []:
        if not isinstance(step, dict):
            continue
        run = step.get("run")
        if isinstance(run, str) and "requirements.txt" in run:
            return True
    return False


def _needs_lxml(root: Path, path: Path, seen: set[Path]) -> bool:
    path = path.resolve()
    if path in seen or not path.is_file():
        return False
    seen.add(path)
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return False
    if _LXML.search(text):
        return True
    for module in _imported_modules(text):
        resolved = _module_path(root, module)
        if resolved is not None and _needs_lxml(root, resolved, seen):
            return True
    for rel in _referenced_scripts(text):
        if _needs_lxml(root, root / rel, seen):
            return True
    return False


def _imported_modules(text: str) -> list[str]:
    modules: list[str] = []
    for match in _IMPORT.finditer(text):
        name = match.group(1) or match.group(2) or ""
        root_name = name.split(".", 1)[0]
        if root_name in {"modules", "scripts", "config"}:
            modules.append(name)
    return modules


def _module_path(root: Path, module: str) -> Path | None:
    parts = module.split(".")
    file_path = root.joinpath(*parts).with_suffix(".py")
    if file_path.is_file():
        return file_path
    init_path = root.joinpath(*parts, "__init__.py")
    if init_path.is_file():
        return init_path
    return None


def _referenced_scripts(text: str) -> set[str]:
    found = {f"scripts/{name}" for name in _SCRIPT_SLASH.findall(text)}
    found.update(f"scripts/{name}" for name in _SCRIPT_JOIN.findall(text))
    return found


def _yaml_where(exc: BaseException) -> str:
    mark = getattr(exc, "problem_mark", None)
    problem = getattr(exc, "problem", None) or exc.__class__.__name__
    if mark is None:
        return str(problem)
    return f"regel {mark.line + 1}, kolom {mark.column + 1}: {problem}"


if __name__ == "__main__":
    raise SystemExit(main())
