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


class MetadataFailure(unittest.TestCase):
    def test_failed_reads_are_not_serialized(self):
        for role in ['coord', 'datanode', 'datanodeSlave']:
            for failure in ['connect', 'execute', 'fetchall', None]:
                with self.subTest(role=role, failure=failure):
                    conf = NS(coordPorts=[], coordSlavePorts=[], datanodePorts=[], datanodeSlavePorts=[],
                              cn_pgxc_node=[], cn_pgxc_node_file=[], dn_pgxc_node=[], dn_pgxc_node_file=[],
                              dn_slave_pgxc_node=[], dn_slave_pgxc_node_file=[], DumpToPickleFile=Mock())
                    setattr(conf, role + 'Ports', [12345])
                    db = Mock()
                    cursor = db.connect.return_value.cursor.return_value
                    error = RuntimeError('catalog unavailable')
                    if failure == 'connect':
                        db.connect.side_effect = error
                    elif failure:
                        getattr(cursor, failure).side_effect = error
                    ns = dict(PgxcCtlConf=NS(GeneratePgxcCtlConfNoPortsInPgclass=lambda *args: conf),
                              os=Mock(), time=Mock(), getpass=NS(getuser=lambda: 'tester'),
                              psycopg2=db, global_thl_data=NS(logger=Mock()))
                    run = load('clustermgr.py', ['CreateBaseCluster'], ns)['CreateBaseCluster']
                    if failure:
                        with self.assertRaisesRegex(RuntimeError, 'catalog unavailable'):
                            run(NS(name=lambda: 'case'), '/unused')
                        conf.DumpToPickleFile.assert_not_called()
                    else:
                        run(NS(name=lambda: 'case'), '/unused')
                        conf.DumpToPickleFile.assert_called_once()
                    self.assertIn("stop all", ns["os"].system.call_args.args[0])

if __name__ == "__main__":
    unittest.main()
