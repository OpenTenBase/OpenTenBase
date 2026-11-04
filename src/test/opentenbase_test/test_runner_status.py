# Copyright (C) 2026 OpenTenBase Authors.
# SPDX-License-Identifier: BSD-3-Clause

import subprocess
import sys
import unittest
from pathlib import Path


# Load the real controller and final invocation without the module's top-level
# directory cleanup and database setup. Only discovery and task execution are
# replaced; result aggregation, reporting and process exit run unchanged.
PROBE = r'''
import ast
import sys
from types import SimpleNamespace

source = sys.argv[1]
tree = ast.parse(open(source, encoding="utf-8").read(), filename=source)
controller = next(node for node in tree.body
                  if isinstance(node, ast.ClassDef)
                  and node.name == "OpenTenBaseTestController")
cases = [SimpleNamespace(name=name) for name in sys.argv[2].split(",") if name]
global_passed_set = set(filter(None, sys.argv[3].split(",")))
global_args = SimpleNamespace(skip_create_base_cluster=True)
global_thl_data = SimpleNamespace(logger=SimpleNamespace(info=print, error=print))
g_test_dir = g_result_dir = "unused"

class TestCaseMgr:
    def __init__(self, *args):
        self.test_case_list = cases
        self.unique_conf = set()
        self._skip_case = 0
    def GetAllTestCases(self, *args): pass
    def PrintAllTestCases(self): pass
    def GetAllUniqueClusterConf(self): pass

exec(compile(ast.Module(body=[controller], type_ignores=[]), source, "exec"))
OpenTenBaseTestController.CreateAllTask = lambda self: None
OpenTenBaseTestController.RunAllTask = lambda self: None
exec(compile(ast.Module(body=tree.body[-2:], type_ignores=[]), source, "exec"))
'''


class RunnerExitStatusTests(unittest.TestCase):
    def run_summary(self, cases, passed):
        source = Path(__file__).with_name('opentenbase_test.py')
        return subprocess.run(
            [sys.executable, '-c', PROBE, str(source), cases, passed],
            capture_output=True, text=True,
        )

    def test_one_failed_case_returns_failure(self):
        result = self.run_summary('passed,failed', 'passed')
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn('1 failed tests:', result.stdout)
        self.assertIn('failed\n', result.stdout)

    def test_all_failed_cases_return_failure(self):
        result = self.run_summary('first,second', '')
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn('2 failed tests:', result.stdout)

    def test_all_passed_cases_return_success(self):
        result = self.run_summary('first,second', 'first,second')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertNotIn('failed tests:', result.stdout)

    def test_empty_selection_preserves_success(self):
        result = self.run_summary('', '')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == '__main__':
    unittest.main()
