"""De workflow-lint moet het incident van 18 november 2026 tegenhouden."""

from __future__ import annotations

from pathlib import Path

from scripts.lint_github_workflows import (
    job_dependency_problems,
    lint_repo,
    lint_workflow,
)

ROOT = Path(__file__).resolve().parents[1]


def test_current_workflows_pass() -> None:
    assert lint_repo(ROOT) == []


def test_column_zero_heredoc_is_rejected(tmp_path: Path) -> None:
    workflow_dir = tmp_path / ".github" / "workflows"
    workflow_dir.mkdir(parents=True)
    broken = workflow_dir / "ktm_new_products_daily.yml"
    broken.write_text(
        """
name: ktm_new_products_daily
on:
  schedule:
    - cron: "0 4 * * *"
  workflow_dispatch:
jobs:
  daily:
    runs-on: ubuntu-latest
    steps:
      - run: |
          echo ok
from pathlib import Path
""",
        encoding="utf-8",
    )
    problems = lint_workflow(tmp_path, broken)
    assert problems
    assert "ongeldige YAML" in problems[0]


def test_motox_garage_without_lxml_install_is_rejected() -> None:
    job = {
        "steps": [
            {"run": 'pip install "requests>=2.28,<3"'},
            {"run": "python3 scripts/deploy_motox_garage_vercel.py"},
        ]
    }
    problems = job_dependency_problems(ROOT, "motox_ebihr_sync.yml", "garage", job)
    assert problems
    assert "lxml" in problems[0]
    assert "requirements.txt" in problems[0]


def test_remote_vps_script_is_not_treated_as_runner_import() -> None:
    job = {
        "steps": [
            {
                "run": (
                    "ssh host \\\n"
                    "  \\$PY -u scripts/run_new_products_daily.py\n"
                )
            }
        ]
    }
    assert job_dependency_problems(ROOT, "ktm_new_products_daily.yml", "daily", job) == []
