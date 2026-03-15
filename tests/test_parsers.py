"""Tests for slurmigo parser functions (sense + act modules)."""

import os

import pytest

from slurmigo.sense import parse_sbatch_directives
from slurmigo.act import parse_params_file, count_tasks


@pytest.fixture
def script_file(tmp_path):
    """Write a temp sbatch script and return its path."""

    def _write(content: str) -> str:
        p = tmp_path / "run.sh"
        p.write_text(content)
        return str(p)

    return _write


@pytest.fixture
def params_file(tmp_path):
    """Write a temp params file and return its path."""

    def _write(content: str) -> str:
        p = tmp_path / "params.txt"
        p.write_text(content)
        return str(p)

    return _write


class TestParseSbatchDirectives:
    def test_long_form_equals(self, script_file):
        path = script_file("#!/bin/bash\n#SBATCH --partition=main\n")
        result = parse_sbatch_directives(path)
        assert result["partition"] == "main"

    def test_short_form_p(self, script_file):
        path = script_file("#!/bin/bash\n#SBATCH -p main\n")
        result = parse_sbatch_directives(path)
        assert result["partition"] == "main"

    def test_long_form_space_separated(self, script_file):
        path = script_file("#!/bin/bash\n#SBATCH --time 02:00:00\n")
        result = parse_sbatch_directives(path)
        assert result["time"] == "02:00:00"

    def test_empty_script(self, script_file):
        path = script_file("#!/bin/bash\necho hello\n")
        result = parse_sbatch_directives(path)
        assert result == {}

    def test_multiple_directives(self, script_file):
        path = script_file(
            "#!/bin/bash\n"
            "#SBATCH --partition=main\n"
            "#SBATCH --time=01:00:00\n"
            "#SBATCH --mem=4G\n"
        )
        result = parse_sbatch_directives(path)
        assert result["partition"] == "main"
        assert result["time"] == "01:00:00"
        assert result["mem"] == "4G"

    def test_short_form_t(self, script_file):
        path = script_file("#!/bin/bash\n#SBATCH -t 03:00:00\n")
        result = parse_sbatch_directives(path)
        assert result["time"] == "03:00:00"

    def test_missing_file_returns_empty(self):
        result = parse_sbatch_directives("/nonexistent/path/run.sh")
        assert result == {}


class TestParseParamsFile:
    def test_keyvalue_format(self, params_file):
        path = params_file("alpha=0.1 beta=0.5\nalpha=0.2 beta=0.5\n")
        params, fmt = parse_params_file(path)
        assert fmt == "keyvalue"
        assert len(params) == 2
        assert params[0] == {"alpha": "0.1", "beta": "0.5"}

    def test_csv_format(self, params_file):
        path = params_file("alpha,beta\n0.1,0.5\n0.2,0.5\n")
        params, fmt = parse_params_file(path)
        assert fmt == "csv"
        assert len(params) == 2
        assert params[0]["alpha"] == "0.1"
        assert params[0]["beta"] == "0.5"

    def test_skips_comments_and_blank_lines(self, params_file):
        path = params_file("# header comment\n\nalpha=0.1\n\n# another\nalpha=0.2\n")
        params, fmt = parse_params_file(path)
        assert len(params) == 2

    def test_empty_file_returns_empty(self, params_file):
        path = params_file("")
        params, fmt = parse_params_file(path)
        assert params == []


class TestCountTasks:
    def test_keyvalue_counts_correctly(self, params_file):
        path = params_file("a=1\na=2\na=3\n")
        assert count_tasks(path) == 3

    def test_csv_subtracts_header(self, params_file):
        path = params_file("col1,col2\nval1,val2\nval3,val4\n")
        assert count_tasks(path) == 2

    def test_skips_comments_and_blanks(self, params_file):
        path = params_file("# comment\n\na=1\na=2\n# another\n")
        assert count_tasks(path) == 2

    def test_empty_file(self, params_file):
        path = params_file("")
        assert count_tasks(path) == 0
