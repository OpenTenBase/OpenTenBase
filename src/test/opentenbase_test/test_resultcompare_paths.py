# Copyright (c) 2026 OpenTenBase Authors. All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause

"""Compare real result files without interpreting their paths as shell text."""
import os
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from resultcompare import ResultCompare


class ResultPathTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if shutil.which('diff') is None:
            raise unittest.SkipTest('the existing diff executable is required')

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='otb-result-paths-')
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def comparison(self, expected='same\n', actual='same\n', input_dir='inputs',
                   output_dir='output', name='case', report='summary.diff',
                   expected_name='expected.result'):
        inputs = self.root / input_dir
        outputs = self.root / output_dir
        inputs.mkdir(exist_ok=True)
        outputs.mkdir(exist_ok=True)
        expect_path = inputs / expected_name
        result_path = inputs / 'actual.result'
        expect_path.write_text(expected, encoding='utf-8')
        result_path.write_text(actual, encoding='utf-8')
        summary = self.root / report
        comparator = ResultCompare(name, str(expect_path), str(result_path),
                                   str(summary), str(outputs))
        return comparator, summary, outputs

    def test_equal_plain_files_still_match(self):
        comparator, summary, output = self.comparison()
        self.assertTrue(comparator.compare())
        self.assertFalse(summary.exists())
        self.assertFalse((output / 'case.diff').exists())

    def test_different_plain_files_still_report_a_difference(self):
        comparator, summary, _ = self.comparison('expected\n', 'actual\n')
        self.assertFalse(comparator.compare())
        report = summary.read_bytes()
        self.assertIn(b'-expected', report)
        self.assertIn(b'+actual', report)

    def test_spaces_in_input_directory_do_not_change_equal_results(self):
        comparator, _, _ = self.comparison(input_dir='result files')
        self.assertTrue(comparator.compare())

    def test_spaces_in_output_directory_do_not_change_equal_results(self):
        comparator, _, _ = self.comparison(output_dir='comparison output')
        self.assertTrue(comparator.compare())

    def test_apostrophe_in_input_filename_is_literal(self):
        comparator, _, _ = self.comparison(expected_name="author's.result")
        self.assertTrue(comparator.compare())

    def test_dollar_in_input_filename_is_literal(self):
        comparator, _, _ = self.comparison(expected_name='amount$OTB_PATH_TEST.result')
        with patch.dict(os.environ, {'OTB_PATH_TEST': 'expanded'}):
            self.assertTrue(comparator.compare())

    def test_apostrophe_and_space_in_case_name_are_literal(self):
        comparator, _, output = self.comparison(name="author's case")
        self.assertTrue(comparator.compare())
        self.assertFalse((output / "author's case.diff").exists())

    def test_difference_appends_to_literal_report_path(self):
        comparator, summary, output = self.comparison(
            'expected\n', 'actual\n', report='summary results.diff')
        summary.write_bytes(b'previous case\n')
        self.assertFalse(comparator.compare())
        self.assertEqual(summary.read_bytes(),
                         b'previous case\n' + (output / 'case.diff').read_bytes())

    def test_regex_match_with_spaced_paths_still_matches(self):
        comparator, summary, output = self.comparison(
            '--?value: [0-9]+\nrequired row\n', 'value: 42\nrequired row\n',
            input_dir='result files', output_dir='comparison output')
        self.assertTrue(comparator.compare())
        self.assertFalse(summary.exists())
        self.assertFalse((output / 'case.diff').exists())

    def test_regex_mismatch_with_spaced_report_keeps_the_difference(self):
        comparator, summary, _ = self.comparison(
            '--?value: [0-9]+\n', 'value: invalid\n',
            report='summary results.diff')
        self.assertFalse(comparator.compare())
        self.assertIn(b'value: invalid', summary.read_bytes())

    def test_missing_diff_still_returns_false(self):
        comparator, _, _ = self.comparison()
        empty_path = self.root / 'empty-bin'
        empty_path.mkdir()
        with patch.dict(os.environ, {'PATH': str(empty_path)}):
            self.assertFalse(comparator.compare())


if __name__ == '__main__':
    unittest.main()
