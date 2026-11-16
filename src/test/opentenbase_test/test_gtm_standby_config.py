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


class GtmStandby(unittest.TestCase):
    def test_generated_config(self):
        ns = dict(os=os, getpass=__import__('getpass'), AllocatePorts=lambda _: 10000,
                  global_thl_data=NS(logger=Mock()))
        cls = load('clustermgr.py', ['PgxcCtlConf', 'ToBashArray'], ns)['PgxcCtlConf']
        oldcwd = os.getcwd()
        try:
            os.chdir(ROOT)
            for standby in [0, 1]:
                with self.subTest(standby=standby), tempfile.TemporaryDirectory() as tmp:
                    conf = cls(NS(cn=1, dn=1, cn_slave=0, dn_slave=0, gtm_slave=standby), tmp)
                    conf.SetMemberVariables()
                    conf.WriteToFile()
                    text = (Path(tmp) / 'pgxc_ctl.conf').read_text()
                    expected = 'y' if standby else 'n'
                    self.assertIn('gtmSlave=' + expected + '\n', text)
                    self.assertNotIn('_TABSE_TEST_', text)
        finally:
            os.chdir(oldcwd)

if __name__ == "__main__":
    unittest.main()
