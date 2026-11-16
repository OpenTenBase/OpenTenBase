# Copyright (C) 2026 OpenTenBase Authors. All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
"""Database-free regression check; run this file with Python 3.
Extract real definitions to avoid the runner's import-time destructive setup.
An optional argument selects a different source directory for before/after checks.
"""
import ast
import os
import re
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import Mock
from datetime import datetime

ROOT = Path(sys.argv.pop(1)) if len(sys.argv) > 1 else Path(__file__).resolve().parent

def load(filename, names, namespace):
    path = ROOT / filename
    tree = ast.parse(path.read_text())
    nodes = [n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.ClassDef)) and n.name in names]
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), 'exec'), namespace)
    return namespace


class WorkerCleanup(unittest.TestCase):
    def test_success_and_failure(self):
        ns = dict(threading=__import__('threading'), OpenTenBaseLogger=Mock(), global_log_dir='./')
        cls = load('opentenbase_test.py', ['ThreadTask'], ns)['ThreadTask']
        for failed in [False, True]:
            with self.subTest(failed=failed):
                events = []
                task = cls('cleanup')
                task.test = NS(name='case')
                task.SetupCluster = lambda: events.append('setup')
                task.ShutdownCluster = lambda: events.append('shutdown')
                def run():
                    events.append('run')
                    if failed:
                        raise ValueError('test execution failed')
                task.RunSingleTest = run
                if failed:
                    with self.assertRaisesRegex(ValueError, 'test execution failed'):
                        task.run()
                else:
                    task.run()
                self.assertEqual(events, ['setup', 'run', 'shutdown'])

if __name__ == "__main__":
    unittest.main()
