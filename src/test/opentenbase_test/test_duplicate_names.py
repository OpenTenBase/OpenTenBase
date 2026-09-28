# Copyright (C) 2026 OpenTenBase Authors.
# SPDX-License-Identifier: BSD-3-Clause
import importlib.util
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


class DuplicateNamesTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.tests = self.root / 't'
        self.results = self.root / 'r'
        self.tests.mkdir()
        self.results.mkdir()
        # Import the real discovery module with only logging/config dependencies
        # stubbed; no database or runner startup is performed.
        common = types.ModuleType('common')
        common.global_thl_data = types.SimpleNamespace(logger=Mock())
        common.ClusterConf = lambda *counts: tuple(counts)
        spec = importlib.util.spec_from_file_location(
            'case_manager_under_test', Path(__file__).with_name('testcasemgr.py'))
        self.module = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {'common': common}):
            spec.loader.exec_module(self.module)
        self.manager = self.module.TestCaseMgr(str(self.tests), str(self.results))

    def add_case(self, relative):
        path = self.tests / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('# no database commands\n')
        return path

    def test_nested_duplicate_rejected_with_both_paths(self):
        first = self.add_case('first/same.test')
        second = self.add_case('second/same.test')
        self.manager.GetAllTestCases(str(first))
        with self.assertRaises(ValueError) as error:
            self.manager.GetAllTestCases(str(second))
        self.assertIn(str(first), str(error.exception))
        self.assertIn(str(second), str(error.exception))
        self.assertEqual(len(self.manager.test_case_list), 1)

    def test_unique_nested_names_preserved(self):
        self.add_case('first/one.test')
        self.add_case('second/two.test')
        self.manager.GetAllTestCases(str(self.tests))
        self.assertEqual([case.name for case in self.manager.test_case_list], ['one', 'two'])

    def test_non_test_files_do_not_reserve_names(self):
        self.add_case('first/same.conf')
        self.add_case('second/same.test')
        self.manager.GetAllTestCases(str(self.tests))
        self.assertEqual([case.name for case in self.manager.test_case_list], ['same'])


if __name__ == '__main__':
    unittest.main()
