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


class NodeNumbers(unittest.TestCase):
    def test_zero_does_not_select_last_node(self):
        cls = load('clustermgr.py', ['PgxcCtlConf'], dict(re=re))['PgxcCtlConf']
        conf = cls(None, '/unused')
        conf.coordPorts, conf.datanodePorts = [101, 102], [201, 202]
        conf.coordSlavePorts, conf.datanodeSlavePorts = [301, 302], [401, 402]
        conf.coordForwardPorts, conf.datanodeForwardPorts = [501, 502], [601, 602]
        for name in ['cn0', 'dn0', 'cn0slave1', 'dn0slave1', 'CN_0']:
            with self.subTest(name=name), self.assertRaises(ValueError):
                conf.GetNodePortInPgclass(name)
        for name in ['cn0', 'dn0']:
            with self.subTest(forward=name), self.assertRaises(ValueError):
                conf.GetNodeForwardPortInPgclass(name)
        self.assertEqual(conf.GetNodePortInPgclass('cn1'), 101)
        self.assertEqual(conf.GetNodePortInPgclass('dn2'), 202)
        self.assertEqual(conf.GetNodePortInPgclass('cn2slave1'), 302)
        self.assertEqual(conf.GetNodeForwardPortInPgclass('dn2'), 602)

if __name__ == "__main__":
    unittest.main()
