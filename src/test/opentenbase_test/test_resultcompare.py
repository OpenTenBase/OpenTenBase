# Copyright (C) 2026 OpenTenBase Authors.
# SPDX-License-Identifier: BSD-3-Clause

import os
import tempfile
import unittest
from pathlib import Path

from resultcompare import ResultCompare


class ResultCompareTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.previous_directory = os.getcwd()
        os.chdir(self.directory.name)
        self.addCleanup(self.directory.cleanup)
        self.addCleanup(os.chdir, self.previous_directory)
        Path('tmp').mkdir()

    def compare(self, expected, actual):
        Path('expected').write_text(expected)
        Path('actual').write_text(actual)
        return ResultCompare('case', 'expected', 'actual', 'summary.diff', 'tmp').compare()

    def test_missing_trailing_output_after_regex_match(self):
        self.assertFalse(self.compare('--?value: [0-9]+\nrequired row\n', 'value: 42\n'))
        self.assertIn('required row', Path('summary.diff').read_text())

    def test_missing_trailing_regex(self):
        self.assertFalse(self.compare('--?value: [0-9]+\n--?.*', 'value: 42\n'))

    def test_complete_regex_output(self):
        self.assertTrue(self.compare('--?value: [0-9]+\nrequired row\n', 'value: 42\nrequired row\n'))

    def test_extra_output(self):
        self.assertFalse(self.compare('--?value: [0-9]+\n', 'value: 42\nextra row\n'))

    def test_regex_mismatch(self):
        self.assertFalse(self.compare('--?value: [0-9]+\n', 'value: unknown\n'))

    def test_empty_output(self):
        self.assertFalse(self.compare('--?.*\n', ''))

    def test_exact_output(self):
        self.assertTrue(self.compare('value: 42\n', 'value: 42\n'))


if __name__ == '__main__':
    unittest.main()
