# Copyright (C) 2026 OpenTenBase Authors.
# SPDX-License-Identifier: BSD-3-Clause

import ast
import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch


class MissingResultTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.expected = root / 'case.result'
        test_file = root / 'case.test'
        test_file.write_text('# No database commands in this runner test.\n')
        self.logger = Mock()
        self.aggregate = Mock()
        self.compare = Mock()
        self.compare.return_value.compare.return_value = True
        self.args = SimpleNamespace(record=False, debug=False)
        self.task = SimpleNamespace(test=SimpleNamespace(
            name='case', test_file=str(test_file), result_file=str(self.expected)))

        # Execute the real method without importing destructive module-level
        # cluster/directory setup. DB configuration and comparison are replaced;
        # files and result decisions are real, with logging/aggregation spies.
        source = Path(__file__).with_name('opentenbase_test.py')
        tree = ast.parse(source.read_text(encoding='utf-8'), filename=str(source))
        cls = next(n for n in tree.body
                   if isinstance(n, ast.ClassDef) and n.name == 'ThreadTask')
        method = next(n for n in cls.body
                      if isinstance(n, ast.FunctionDef) and n.name == 'RunSingleTest')
        scope = dict(os=os, datetime=datetime, global_args=self.args,
                     global_base_dir=str(root), global_output_dir=str(root),
                     global_thl_data=SimpleNamespace(logger=self.logger),
                     PgxcCtlConf=Mock(), ResultCompare=self.compare,
                     ResultAgg=self.aggregate)
        exec(compile(ast.Module(body=[method], type_ignores=[]), str(source), 'exec'), scope)
        self.run_case = lambda: scope['RunSingleTest'](self.task)

    def test_missing_expected_file_does_not_pass(self):
        self.run_case()
        self.aggregate.assert_not_called()
        self.compare.assert_not_called()
        self.assertTrue(any(str(self.expected) in call.args[0]
                            for call in self.logger.error.call_args_list))
        self.assertFalse(self.expected.exists())

    def test_directory_is_not_an_expected_file(self):
        self.expected.mkdir()
        self.run_case()
        self.compare.assert_not_called()
        self.aggregate.assert_not_called()

    def test_existing_matching_result_passes(self):
        self.expected.write_text('')
        self.run_case()
        self.compare.assert_called_once()
        self.aggregate.assert_called_once_with('case')

    def test_existing_mismatch_does_not_pass(self):
        self.expected.write_text('expected output\n')
        self.compare.return_value.compare.return_value = False
        self.run_case()
        self.compare.assert_called_once()
        self.aggregate.assert_not_called()

    def test_record_mode_creates_missing_result(self):
        self.args.record = True
        # The parent directory already exists; suppress the POSIX mkdir guard.
        with patch.object(os, 'system'):
            self.run_case()
        self.assertTrue(self.expected.is_file())
        self.assertEqual(self.expected.read_text(), '')
        self.compare.assert_not_called()
        self.aggregate.assert_called_once_with('case')


if __name__ == '__main__':
    unittest.main()
