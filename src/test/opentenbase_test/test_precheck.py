# Copyright (C) 2026 OpenTenBase Authors.
# SPDX-License-Identifier: BSD-3-Clause

import os
import unittest
from unittest.mock import Mock, patch

import precheck
from common import CommandResult, global_thl_data


class PreCheckTest(unittest.TestCase):
    def setUp(self):
        self.previous_logger = getattr(global_thl_data, 'logger', None)
        global_thl_data.logger = Mock()

    def tearDown(self):
        if self.previous_logger is None:
            del global_thl_data.logger
        else:
            global_thl_data.logger = self.previous_logger

    def run_check(self, installed_hash, source_hash):
        results = [CommandResult(b'../../backend/postgres', b'', '', 0),
                   CommandResult(installed_hash + b'  installed/postgres', b'', '', 0),
                   CommandResult(source_hash + b'  ../../backend/postgres', b'', '', 0)]
        with patch.dict(os.environ, {'OPENTENBASE_HOME': '/installed'}), \
                patch.object(precheck, 'check_executable_in_OPENTENBASE_HOME'), \
                patch.object(precheck, 'ExecCommand', side_effect=results), \
                patch('time.sleep') as sleep:
            precheck.PreCheck.check()
        return sleep

    def test_mismatched_binaries_wait_after_warning(self):
        sleep = self.run_check(b'1111', b'2222')
        sleep.assert_called_once_with(3600)
        global_thl_data.logger.warning.assert_called_once()

    def test_matching_binaries_do_not_wait(self):
        sleep = self.run_check(b'1111', b'1111')
        sleep.assert_not_called()
        global_thl_data.logger.warning.assert_not_called()


if __name__ == '__main__':
    unittest.main()
