# Copyright (c) 2026 OpenTenBase Authors. All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause

"""Executable discovery must identify the selected installation directory."""

import logging
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import precheck
from common import global_thl_data


class ExecutableRootTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="otb-precheck-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.home = self.root / "otb"
        self.binary = self.create_binary(self.home / "bin" / "postgres")
        self.previous_logger = getattr(global_thl_data, "logger", None)
        global_thl_data.logger = logging.getLogger("precheck-path-tests")
        self.addCleanup(self.restore_logger)

    def restore_logger(self):
        if self.previous_logger is None:
            del global_thl_data.logger
        else:
            global_thl_data.logger = self.previous_logger

    def create_binary(self, path):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("fixture: executable discovery is stubbed\n", encoding="utf-8")
        return path

    def check(self, home, binary, accepted):
        """Only stub the external `which` result; resolve real fixture paths."""
        completed = subprocess.CompletedProcess(
            ["which", "postgres"], 0 if binary else 1,
            stdout=(str(binary) + "\n").encode() if binary else b"",
        )
        with patch.dict(os.environ, {"OPENTENBASE_HOME": str(home)}), patch.object(
            precheck.subprocess, "run", return_value=completed
        ) as discover:
            if accepted:
                precheck.check_executable_in_OPENTENBASE_HOME("postgres")
            else:
                with self.assertRaises(SystemExit) as result:
                    precheck.check_executable_in_OPENTENBASE_HOME("postgres")
                self.assertEqual(result.exception.code, 1)
            discover.assert_called_once_with(["which", "postgres"], stdout=subprocess.PIPE)

    def test_binary_in_selected_installation(self):
        self.check(self.home, self.binary, True)

    def test_hyphenated_sibling_installation_is_rejected(self):
        sibling = self.create_binary(self.root / "otb-old" / "bin" / "postgres")
        self.check(self.home, sibling, False)

    def test_numeric_sibling_installation_is_rejected(self):
        sibling = self.create_binary(self.root / "otb2" / "bin" / "postgres")
        self.check(self.home, sibling, False)

    def test_parent_segments_cannot_escape_selected_installation(self):
        self.create_binary(self.root / "outside" / "bin" / "postgres")
        self.check(self.home, str(self.home) + "/../outside/bin/postgres", False)

    def test_relative_installation_directory_is_resolved(self):
        self.check(os.path.relpath(self.home), self.binary, True)

    def test_equivalent_dot_segments_are_accepted(self):
        self.check(str(self.home) + "/./", self.binary, True)

    def test_trailing_separator_is_accepted(self):
        self.check(str(self.home) + os.sep, self.binary, True)

    def test_unrelated_installation_is_rejected(self):
        outside = self.create_binary(self.root / "outside" / "bin" / "postgres")
        self.check(self.home, outside, False)

    def test_installation_directory_with_spaces_is_accepted(self):
        home = self.root / "installation with spaces"
        binary = self.create_binary(home / "bin" / "postgres")
        self.check(home, binary, True)

    def test_missing_executable_is_rejected(self):
        self.check(self.home, None, False)


if __name__ == "__main__":
    unittest.main()
