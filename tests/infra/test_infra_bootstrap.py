"""Static checks of bootstrap.sh (it is never executed here: it would touch GCP)."""
import re
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "bootstrap.sh"
TEXT = SCRIPT.read_text(encoding="utf-8")


def test_script_is_a_safe_dry_run_by_default():
    assert TEXT.startswith("#!/usr/bin/env bash") and "set -euo pipefail" in TEXT
    assert "APPLY=false" in TEXT and 'if $APPLY; then "$@"; fi' in TEXT
    assert "--apply" in TEXT and "--with-iam" in TEXT


def test_script_never_deletes_or_destroys():
    executable = TEXT.split("cat <<EOF")[0]  # the heredoc afterwards is printed advice, not executed
    code = "\n".join(line for line in executable.splitlines() if not line.lstrip().startswith("#"))
    assert not re.search(r"\b(delete|destroy|rm -rf|--force|rb|truncate)\b", code.replace('"type":"Delete"', ""), flags=re.I)


def test_iam_is_review_only_unless_explicitly_requested():
    assert 'if $WITH_IAM && $APPLY; then "$@"; else echo "(review only) $*"; fi' in TEXT
    assert "roles/owner" not in TEXT and "roles/editor" not in TEXT


def test_required_resources_and_guardrails_are_present():
    for needle in ("rides-events", "rides-events-sub", "RAW_RETENTION_DAYS=14", '"raw/"', "--environment-size small",
                   "--budget-amount=50USD", "--budget-amount=100USD", "--uniform-bucket-level-access", "sa-rh-spark"):
        assert needle in TEXT, needle
    assert "sql/ddl/*.sql" in TEXT and sorted((ROOT / "sql" / "ddl").glob("*.sql"))


def test_composer_and_budgets_are_printed_not_run():
    tail = TEXT[TEXT.index("8. Printed only"):]
    assert "cat <<EOF" in tail and "run gcloud composer" not in TEXT and "run gcloud billing" not in TEXT


def test_no_credentials_in_the_script():
    assert not re.search(r"(?i)(password|secret|token|private_key)\s*=", TEXT)


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash not available: syntax not checked")
def test_bash_syntax():
    done = subprocess.run(["bash", "-n", "bootstrap.sh"], capture_output=True, text=True, cwd=ROOT)
    assert done.returncode == 0, done.stderr
