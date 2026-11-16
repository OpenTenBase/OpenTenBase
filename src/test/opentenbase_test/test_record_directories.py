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


class RecordDirectories(unittest.TestCase):
    def test_nested_and_spaced_paths(self):
        for relative in ['nested/deep/case.result', 'with spaces/deep/case.result', 'case.result']:
            with self.subTest(path=relative), tempfile.TemporaryDirectory() as tmp:
                source = Path(tmp) / 'empty.test'
                source.write_text('')
                result = Path(tmp) / relative
                ns = dict(os=os, datetime=datetime, threading=__import__('threading'),
                          global_args=NS(record=True, debug=False), global_output_dir=tmp,
                          global_base_dir=tmp, global_thl_data=NS(logger=Mock()),
                          PgxcCtlConf=NS(LoadFromPickleFile=lambda _: None), ResultAgg=Mock())
                cls = load('opentenbase_test.py', ['ThreadTask'], ns)['ThreadTask']
                for repeat in range(2):
                    task = cls('record')
                    task.test = NS(name='empty', test_file=str(source), result_file=str(result))
                    task.RunSingleTest()
                    self.assertEqual(result.read_text(), '')

if __name__ == "__main__":
    unittest.main()
