# Copyright (C) 2026 OpenTenBase Authors. All rights reserved.
# Licensed under the BSD-3-Clause License; see LICENSE.txt.

from pathlib import Path
import shlex
import tempfile
import unittest
from unittest.mock import Mock, patch

import clustermgr


class ChangeConfTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='changeconf ')
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.logger = patch.object(clustermgr.global_thl_data, 'logger', Mock(),
                                   create=True)
        self.logger.start()
        self.addCleanup(self.logger.stop)

    def make_node(self, role, name, content='work_mem = 4MB\n'):
        directory = self.base / 'database' / role / name
        directory.mkdir(parents=True)
        (directory / 'postgresql.conf').write_text(content)
        return directory

    def test_updates_all_node_roles_before_reloading_their_full_paths(self):
        nodes = [self.make_node(role, name) for role, name in (
            ('coord', 'coord_cn1'), ('coord_slave', 'coord_cn1'),
            ('dn_master', 'dn1'), ('dn_slave', 'dn1'))]

        def reload_node(command):
            # Only pg_ctl is simulated; all configuration files are real.
            for node in nodes:
                text = (node / 'postgresql.conf').read_text()
                self.assertIn('work_mem=16MB\n', text)
                self.assertNotIn('work_mem = 4MB', text)
                self.assertIn('# added by opentenbase_test test case\n', text)

        with patch.object(clustermgr, 'ExecCommand', side_effect=reload_node) as run:
            clustermgr.ChangeConf(str(self.base), 'work_mem=16MB')

        self.assertEqual([shlex.split(call.args[0]) for call in run.call_args_list],
                         [['pg_ctl', '-D', str(node), 'reload'] for node in nodes])

    def test_works_when_optional_replica_directories_are_absent(self):
        nodes = [self.make_node('coord', 'coord_cn1'),
                 self.make_node('dn_master', 'dn1')]
        with patch.object(clustermgr, 'ExecCommand') as run:
            clustermgr.ChangeConf(str(self.base), 'work_mem=16MB')
        self.assertEqual(run.call_count, len(nodes))
        for node in nodes:
            self.assertIn('work_mem=16MB\n',
                          (node / 'postgresql.conf').read_text())

    def test_preserves_other_settings_and_commented_defaults(self):
        content = ("# work_mem = 4MB\n"
                   "maintenance_work_mem = 64MB\n"
                   "log_line_prefix = 'work_mem=%m'\n"
                   "  work_mem = 4MB\n"
                   "work_mem=8MB")
        node = self.make_node('coord', 'coord_cn1', content)
        with patch.object(clustermgr, 'ExecCommand'):
            clustermgr.ChangeConf(str(self.base), 'work_mem=16MB')
        text = (node / 'postgresql.conf').read_text()
        self.assertEqual(text, "# work_mem = 4MB\n"
                         "maintenance_work_mem = 64MB\n"
                         "log_line_prefix = 'work_mem=%m'\n"
                         "\n# added by opentenbase_test test case\nwork_mem=16MB\n")

    def test_ignores_non_directory_entries(self):
        node = self.make_node('coord', 'coord_cn1')
        (node.parent / 'notes.txt').write_text('not a node')
        with patch.object(clustermgr, 'ExecCommand') as run:
            clustermgr.ChangeConf(str(self.base), 'work_mem=16MB')
        self.assertEqual(run.call_count, 1)
        self.assertEqual(shlex.split(run.call_args.args[0]),
                         ['pg_ctl', '-D', str(node), 'reload'])

    def test_no_nodes_do_not_trigger_reload(self):
        for role in ('coord', 'coord_slave', 'dn_master', 'dn_slave'):
            (self.base / 'database' / role).mkdir(parents=True)
        with patch.object(clustermgr, 'ExecCommand') as run:
            clustermgr.ChangeConf(str(self.base), 'work_mem=16MB')
        run.assert_not_called()


if __name__ == '__main__':
    unittest.main()
