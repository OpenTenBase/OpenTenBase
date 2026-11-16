# Copyright (C) 2026 OpenTenBase Authors. All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
# See LICENSE.txt in the repository root.

"""Exercise the actual RunSingleTest method without starting a database.

Usage: python3 src/test/opentenbase_test/tests/test_async_sql.py
An optional argument selects another opentenbase_test.py for before/after checks.
AST extraction avoids that module's import-time directory cleanup and CLI parsing.
Only cluster configuration, logging, and SQL transport are replaced with doubles.
"""
import ast
from datetime import datetime
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
from unittest.mock import Mock


def main():
    source = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parents[1] / "opentenbase_test.py"
    tree = ast.parse(source.read_text())
    task_class = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'ThreadTask')
    method = next(n for n in task_class.body if isinstance(n, ast.FunctionDef) and n.name == 'RunSingleTest')
    module = ast.Module(body=[method], type_ignores=[])

    queries = ["select '100%';", "select '#tag';", "select 10 % 3;", "select 5 # 3;", "select 1;"]
    failures = 0
    with tempfile.TemporaryDirectory() as tmp:
        namespace = dict(os=os, datetime=datetime,
                         global_args=SimpleNamespace(record=False, debug=False),
                         global_output_dir=tmp, global_base_dir=tmp,
                         global_thl_data=SimpleNamespace(logger=Mock()),
                         PgxcCtlConf=SimpleNamespace(LoadFromPickleFile=lambda _: None),
                         ResultAgg=Mock())
        exec(compile(module, str(source), 'exec'), namespace)
        for command in ['send', 'sendblock', '']:
            for sql in queries:
                case = Path(tmp) / 'case.test'
                case.write_text('# comment\n%cn1\n  ' + (command + ' ' if command else '') + sql + '\n')
                captured = []
                def capture(connection, text):
                    captured.append(text.strip())
                task = SimpleNamespace(
                    test=SimpleNamespace(name='case', test_file=str(case), result_file=str(Path(tmp) / 'unused.result')),
                    SendSql=capture, SendBlockSql=capture, ExecuteSqlOneLine=capture)
                namespace['RunSingleTest'](task)
                success = captured == [sql]
                print(('PASS' if success else 'FAIL'), command or 'plain SQL', repr(sql), '=>', repr(captured))
                failures += not success
    print(f'{15 - failures}/15 cases passed')
    sys.exit(bool(failures))


if __name__ == "__main__":
    main()
