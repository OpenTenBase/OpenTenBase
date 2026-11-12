# Copyright (c) 2026 OpenTenBase contributors
# SPDX-License-Identifier: BSD-3-Clause

import os
from pathlib import Path
import sys
import tempfile
import unittest

from loguru import logger
from args import OpenTenBaseTestArgsParser
from common import global_thl_data
import testcasemgr


class SingleTestResultDirectoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / 't' / 'nested').mkdir(parents=True)
        (self.root / 'r' / 'nested').mkdir(parents=True)
        (self.root / 'custom results').mkdir()
        self.old_base = testcasemgr.global_base_dir
        testcasemgr.SetGlobalVar(str(self.root))
        self.addCleanup(testcasemgr.SetGlobalVar, self.old_base)
        self.old_logger = getattr(global_thl_data, 'logger', None)
        global_thl_data.logger = logger
        self.addCleanup(self.restore_logger)

    def restore_logger(self):
        if self.old_logger is None:
            del global_thl_data.logger
        else:
            global_thl_data.logger = self.old_logger

    def discover(self, test_path, result_path, record=False):
        old_argv = sys.argv
        try:
            sys.argv = ['opentenbase_test.py', '-t', str(test_path), '-r', str(result_path)]
            if record:
                sys.argv.append('--record')
            args = OpenTenBaseTestArgsParser.parse()
        finally:
            sys.argv = old_argv
        manager = testcasemgr.TestCaseMgr(os.path.realpath(args.test_dir),
                                        os.path.realpath(args.result_dir))
        manager.GetAllTestCases(os.path.realpath(args.test_dir))
        self.assertEqual(len(manager.test_case_list), 1)
        return Path(manager.test_case_list[0].result_file)

    def make_test(self, relative):
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('%cn 2\n# discovery only\n', encoding='utf-8')
        return path

    def test_custom_directory_for_bundled_single_test(self):
        test = self.make_test('t/nested/example.test')
        expected = self.root / 'custom results/example.result'
        expected.write_text('custom oracle\n', encoding='utf-8')
        self.assertEqual(self.discover(test, expected.parent), expected)

    def test_custom_directory_for_external_single_test(self):
        test = self.make_test('external/example.test')
        expected = self.root / 'custom results/example.result'
        expected.write_text('custom oracle\n', encoding='utf-8')
        self.assertEqual(self.discover(test, expected.parent), expected)

    def test_record_target_in_existing_custom_directory(self):
        test = self.make_test('t/example.test')
        expected = self.root / 'custom results/example.result'
        self.assertFalse(expected.exists())
        self.assertEqual(self.discover(test, expected.parent, record=True), expected)

    def test_default_directory_keeps_nested_mapping(self):
        test = self.make_test('t/nested/example.test')
        self.assertEqual(self.discover(test, self.root / 'r'),
                         self.root / 'r/nested/example.result')

    def test_explicit_result_file_is_preserved(self):
        test = self.make_test('external/example.test')
        expected = self.root / 'custom results/example.result'
        expected.touch()
        self.assertEqual(self.discover(test, expected), expected)

    def test_directory_discovery_keeps_nested_mapping(self):
        self.make_test('t/nested/example.test')
        self.assertEqual(self.discover(self.root / 't', self.root / 'custom results'),
                         self.root / 'custom results/nested/example.result')


if __name__ == '__main__':
    unittest.main()
