#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
# Author: freemeng


if __name__ == "__main__":
    raise Exception("This script cannot be executed directly")

import os
import subprocess

from common import ExecCommand
from common import global_thl_data

def check_env(name):
    if name in os.environ:
        global_thl_data.logger.info(f'{name} is {os.environ[name]}')
    else:
        global_thl_data.logger.error(f'{name} must be set')
        exit(1)




def check_executable_in_OPENTENBASE_HOME(name):
    result = subprocess.run(['which', name], stdout=subprocess.PIPE)
    path = result.stdout.decode().strip()

    if path:
        global_thl_data.logger.info(f'{name} found at {path}')
    else:
        global_thl_data.logger.error(f'{name} not found')
        exit(1)

    if path.startswith(os.environ['OPENTENBASE_HOME']):
        global_thl_data.logger.info(f'{name} in OPENTENBASE_HOME')
    else:
        global_thl_data.logger.error(f'{name} must be in OPENTENBASE_HOME')
        exit(1)

class PreCheck:
    @staticmethod
    def check():
        check_env('OPENTENBASE_HOME')

        check_executable_in_OPENTENBASE_HOME('postgres')
        check_executable_in_OPENTENBASE_HOME('pgxc_ctl')

        g_table_home = os.environ['OPENTENBASE_HOME']

        r = ExecCommand('ls ../../backend/postgres')
        if r.IsSuccess():
            global_thl_data.logger.debug(r.Stdout().strip())
        else:
            global_thl_data.logger.warning('not found ../../backend/postgres')

        r1 = ExecCommand(f'md5sum {g_table_home}/bin/postgres').Stdout().strip().split(' ')[0]
        r2 = ExecCommand(f'md5sum ../../backend/postgres').Stdout().strip().split(' ')[0]

        if r1 != r2:
            global_thl_data.logger.warning('postgres in souce code dir and OPENTENBASE_HOME are different. will sleep 1 hour to notice you')
            time.sleep(3600)
