#!/usr/bin/env python3
# Copyright (C) 2026 OpenTenBase Authors.
# Licensed under BSD-3-Clause; see ../../LICENSE.txt.
"""Check distributed transaction examples via SSH and psql (no dependencies)."""
import argparse
import shlex
import subprocess
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('ssh', 'psql', 'libdir', 'host', 'user'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--port', type=int, default=5432)
    parser.add_argument('--dbname', default='postgres')
    args = parser.parse_args()
    command = 'export LD_LIBRARY_PATH=' + shlex.quote(args.libdir) + '; exec '
    command += shlex.join([args.psql, '-X', '-qAt', '-w', '-v', 'ON_ERROR_STOP=1',
                           '-h', args.host, '-p', str(args.port), '-U', args.user,
                           '-d', args.dbname])
    ssh = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', args.ssh, command]
    schema = 'txn_318_' + uuid.uuid4().hex[:12]
    table = schema + '.accounts'
    sessions = []

    def run(sql):
        result = subprocess.run(ssh, input=sql + '\n', text=True,
                                capture_output=True, timeout=60, check=True)
        return result.stdout.strip()

    def expect(label, actual, expected):
        if actual != expected:
            raise AssertionError(f'{label}: {actual!r} != {expected!r}')
        print(f'PASS {label}: {actual}', flush=True)

    try:
        print('server: ' + run('SELECT version();'), flush=True)
        run(f'CREATE SCHEMA {schema}; CREATE TABLE {table} '
            '(id integer, balance integer) DISTRIBUTE BY SHARD(id);')
        run(f'SET enable_2pc_optimization=off; BEGIN; INSERT INTO {table} '
            'SELECT i,i FROM generate_series(1,1000) g(i); COMMIT;')
        nodes = run("SELECT node_name FROM pgxc_node WHERE node_type='D' "
                    'ORDER BY node_name;').splitlines()
        counts = []
        for node in nodes:
            ident = '"' + node.replace('"', '""') + '"'
            count = int(run(f"EXECUTE DIRECT ON ({ident}) 'SELECT count(*) FROM {table}';"))
            counts.append(count)
            print(f'distribution {node}: {count}', flush=True)
        expect('physical row count', str(sum(counts)), '1000')
        if sum(c > 0 for c in counts) < 2:
            raise AssertionError('Need rows on at least two DNs')
        run(f'SET enable_2pc_optimization=off; BEGIN; UPDATE {table} '
            'SET balance=balance+1; COMMIT;')
        expect('commit count/sum', run(f'SELECT count(*),sum(balance) FROM {table};'),
               '1000|501500')
        run(f'BEGIN; UPDATE {table} SET balance=balance+100; ROLLBACK;')
        expect('rollback sum', run(f'SELECT sum(balance) FROM {table};'), '501500')

        total = 501500
        for isolation in ('REPEATABLE READ', 'READ COMMITTED'):
            # Feed two requests to the same physical reader session; the separate
            # writer finishes between them. No sleeps or timing assumptions.
            session = subprocess.Popen(ssh, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                       text=True)
            sessions.append(session)

            def read_sum(prefix):
                session.stdin.write(prefix + f'SELECT sum(balance) FROM {table};\n')
                session.stdin.flush()
                value = session.stdout.readline().strip()
                if not value:
                    raise RuntimeError('Reader exited before returning a result')
                return value

            expect(isolation + ' initial', read_sum(f'BEGIN ISOLATION LEVEL {isolation};'),
                   str(total))
            run(f'SET enable_2pc_optimization=off; BEGIN; UPDATE {table} '
                'SET balance=balance+1; COMMIT;')
            expected = total if isolation == 'REPEATABLE READ' else total + 1000
            expect(isolation + ' after writer commit', read_sum(''), str(expected))
            session.communicate('ROLLBACK;\n', timeout=30)
            if session.returncode:
                raise RuntimeError('Reader failed')
            total += 1000
        print('PASS all behavioral checks', flush=True)
    finally:
        for session in sessions:
            if session.poll() is None:
                session.kill()
                session.wait()
        run(f'DROP SCHEMA IF EXISTS {schema} CASCADE;')
        print('cleanup: dedicated schema removed', flush=True)


if __name__ == '__main__':
    main()
