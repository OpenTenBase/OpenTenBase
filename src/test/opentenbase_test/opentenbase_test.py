#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
# Author: freemeng
import sys
import os

global_base_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, global_base_dir)

import copy
import time
import random
import threading
import re
import getpass
import socket
import struct
import subprocess
import select
import json
import pickle
import psycopg2
from datetime import datetime

from common import OpenTenBaseLogger, global_thl_data
from common import ExecCommand
from args import OpenTenBaseTestArgsParser
from precheck import PreCheck
from testcasemgr import TestCaseMgr
from clustermgr import CreateBaseCluster
from clustermgr import CloneCluster
from clustermgr import PgxcCtlConf
from clustermgr import ChangeConf
from resultcompare import ResultCompare

import testcasemgr
import clustermgr

global_args = OpenTenBaseTestArgsParser.parse()


os.chdir(global_base_dir)

testcasemgr.SetGlobalVar(global_base_dir)

if global_args.skip_create_base_cluster:
    clustermgr.SetGlobalVar(16000)
else:
    clustermgr.SetGlobalVar(10000)

global_log_dir = './log/'
global_output_dir = './output/'
global_tmp_dir = './tmp/'

os.system('[ -d data ] && rm -rf data')
os.system(f'[ -d {global_log_dir} ] && rm -rf {global_log_dir}')
os.system(f'[ -d {global_output_dir} ] && rm -rf {global_output_dir}')
os.system(f'[ -d {global_tmp_dir} ] && rm -rf {global_tmp_dir}')
os.system(f'mkdir {global_output_dir}')
os.system(f'mkdir {global_tmp_dir}')


OpenTenBaseLogger.CreateLogggerForThisThread(global_log_dir + "opentenbase_test.log")

global_thl_data.logger.info(f'chdir to {global_base_dir}')



g_test_dir = os.path.realpath(global_args.test_dir)
g_result_dir = os.path.realpath(global_args.result_dir)


global_passed_set = set()
global_agg_lock = threading.Lock()
def ResultAgg(passed):
    global global_passed_set
    with global_agg_lock:
        global_passed_set |= {passed}

def GetNodePart(nodeid):
    nodeid = re.sub(r'_', '', nodeid).lower()

    if nodeid == 'gtm':
        return ['gtm', 'master']

    matchObj = re.match(r'^gtmslave(\d+)$', nodeid)
    if matchObj:
        slave = int(matchObj.group(1))
        assert slave == 1
        return ['gtm', 'slave']

    matchObj = re.match(r'^cn(\d+)$', nodeid)
    if matchObj:
        cn = int(matchObj.group(1))
        return [f'cn{cn}', 'master']

    matchObj = re.match(r'^dn(\d+)$', nodeid)
    if matchObj:
        dn = int(matchObj.group(1))
        return [f'dn{dn}', 'master']

    matchObj = re.match(r'^cn(\d+)slave(\d+)$', nodeid)
    if matchObj:
        cn = int(matchObj.group(1))
        slave = int(matchObj.group(2))
        assert slave == 1
        return [f'cn{cn}', 'slave']

    matchObj = re.match(r'^dn(\d+)slave(\d+)$', nodeid)
    if matchObj:
        dn = int(matchObj.group(1))
        slave = int(matchObj.group(2))
        assert slave == 1
        return [f'dn{dn}', 'slave']

    global_thl_data.logger.error(nodeid)
    assert False


class Scheduler:
    def __init__(self, parallel):
        self.parallel = parallel
        if self.parallel < 1:
            self.parallel = 1
        if self.parallel > 100:
            self.parallel = 100

    def run_task(self, task):
        while True:
            running_tasks = threading.enumerate()
            running_test_number = 0
            for t in running_tasks:
                if t.name.startswith('opentenbasetest-'):
                    running_test_number = running_test_number + 1
            if running_test_number < self.parallel:
                break
            time.sleep(1)
        task.start()

class ThreadTask(threading.Thread):
    def __init__(self, name):
        super(ThreadTask, self).__init__(name = name)
        self.name = name
        self.kv_list = {}

    def run(self):
        assert self.test
        OpenTenBaseLogger.CreateLogggerForThisThread(global_log_dir + f'{self.test.name}.log')
        self.SetupCluster()
        self.RunSingleTest()
        self.ShutdownCluster()

    def PatchConf(self):
        test = self.test
        common_conf = f'{os.path.dirname(test.test_file)}/common.conf'
        if os.path.exists(common_conf):
            cmd = f'find data/{test.name} -name "postgresql.conf"'
            r = ExecCommand(cmd)
            for name in r.Stdout().split('\n'):
                name = name.strip()
                if not name:
                    continue
                cmd = f"echo '#patched by testcase(common.conf)' >> {name}; cat {common_conf} >> {name}"
                global_thl_data.logger.debug(cmd)
                ExecCommand(cmd)
        test_conf_4_postgres = f'{test.test_file[0:-5]}.conf'
        if os.path.exists(test_conf_4_postgres):
            cmd = f'find data/{test.name} -name "postgresql.conf"'
            r = ExecCommand(cmd)
            for name in r.Stdout().split('\n'):
                name = name.strip()
                if not name:
                    continue
                cmd = f"echo '#patched by testcase' >> {name}; cat {test_conf_4_postgres} >> {name}"
                global_thl_data.logger.debug(cmd)
                ExecCommand(cmd)

    def SetupCluster(self):
        start_time = datetime.now()
        test = self.test
        src_dir = f'{global_base_dir}/basedata/{test.cluster_conf.name()}'
        dst_dir = f'{global_base_dir}/data/{test.name}'
        CloneCluster(src_dir, dst_dir)

        self.PatchConf()

        cmd = f'pgxc_ctl --home {dst_dir} -c {dst_dir}/pgxc_ctl.conf start all >&/dev/null'
        global_thl_data.logger.info(cmd)
        os.system(cmd)
        time.sleep(2)
        while True:
            cmd = f'pgxc_ctl --home {dst_dir} -c {dst_dir}/pgxc_ctl.conf start all >&/dev/null'
            global_thl_data.logger.info(cmd)
            os.system(cmd)

            time.sleep(5)

            cmd = f'pgxc_ctl --home {dst_dir} -c {dst_dir}/pgxc_ctl.conf monitor all'
            global_thl_data.logger.info(cmd)
            r = ExecCommand(cmd)
            global_thl_data.logger.debug(r)
            if r.IsSuccess() and r.stdout.find('Not') == -1:
                break

        end_time  = datetime.now()
        time_diff = end_time - start_time
        global_thl_data.logger.info(f'start cluster for test {test.name} use time: {time_diff.total_seconds()}')

    def ShutdownCluster(self):
        dst_dir = f'{global_base_dir}/data/{self.test.name}'
        cmd = f'pgxc_ctl --home {dst_dir} -c {dst_dir}/pgxc_ctl.conf stop all >&/dev/null'
        global_thl_data.logger.info(cmd)
        os.system(cmd)

    def RunSingleTest(self):
        test = self.test
        cast_output_file = f'{global_output_dir}/{test.name}.output'
        if global_args.record:
            case_result_dir = os.path.dirname(os.path.abspath(test.result_file))
            os.system(f'[ -d {case_result_dir} ] || mkdir {case_result_dir}')
            self.fout_case = open(f'{test.result_file}', 'w')
        else:
            self.fout_case = open(cast_output_file, 'w')

        start_time = datetime.now()
        basedir = f'{global_base_dir}/data/{test.name}'
        pgxc_ctl_conf = PgxcCtlConf.LoadFromPickleFile(basedir)
        conns = {}
        current_conn = None
        global_thl_data.logger.info(f'run {test}')
        for line_org in open(test.test_file):
            if global_args.debug:
                print(f"\033[33m{line_org}\033[0m", end='')
            # TODO: sql statement contains this, BUG BUG BUG
            line = line_org.split("#")[0].split("%")[0].strip()
            if not line:
                continue

            linesplited = line.split(' ')
            cmd = linesplited[0]
            if cmd == 'connect':
                conn_name = linesplited[1]
                node_id = linesplited[2]
                port = pgxc_ctl_conf.GetNodePortInPgclass(node_id)
                global_thl_data.logger.debug(f'conn name {conn_name} nodeid {node_id}  port {port}')

                try:
                    ON_POSIX = 'posix' in sys.builtin_module_names
                    con = subprocess.Popen(['psql', '-d', 'postgres', '-p', f'{port}', '-a', '--echo-row-delimiter'], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                     stderr=subprocess.PIPE,  close_fds=ON_POSIX)
                except:
                    global_thl_data.logger.error("can not connect to the database or can not get a cursor")
                    assert False
                if conn_name in conns and conns[conn_name]:
                    conns[conn_name].terminate()
                    conns[conn_name].wait()
                    conns[conn_name] = None
                conns[conn_name] = con
                self.receiveoutput(con)
            elif cmd == 'connection':
                conn_name = linesplited[1]
                current_conn = conns[conn_name]
            elif cmd == 'send':
                self.SendSql(current_conn, line[len('send'):] + '\n')
                current_conn = None
            elif cmd == 'sendblock':
                self.SendBlockSql(current_conn, line[len('sendblock'):] + '\n')
                current_conn = None
            elif cmd == 'reap':
                conn_name = linesplited[1]
                current_conn = conns[conn_name]
                self.receiveoutput(current_conn)
            elif cmd == 'disconnect':
                conn_name = linesplited[1]
                if current_conn == conns[conn_name]:
                    current_conn = None
                conns[conn_name].terminate()
                conns[conn_name].wait()
                conns[conn_name] = None
            elif cmd == 'start' or cmd == 'stop' or cmd == 'kill':
                node_id = linesplited[1]
                nn, nt = GetNodePart(node_id)
                if nn.startswith('cn'):
                    cdg = 'coordinator'
                elif nn.startswith('dn'):
                    cdg = 'datanode'
                elif nn.startswith('gtm'):
                    cdg = 'gtm'
                    nn = ''
                else:
                    assert False
                pgxc_cmd = f'pgxc_ctl --home {basedir} -c {basedir}/pgxc_ctl.conf {cmd} {cdg} {nt}  {nn} >&/dev/null'
                os.system(pgxc_cmd)
                global_thl_data.logger.debug(pgxc_cmd)
            elif cmd == 'sleep':
                time.sleep(int(linesplited[1]))
            elif cmd == 'setconfig':
                ChangeConf(f'{global_base_dir}/data/{self.test.name}', ''.join(i for i in linesplited[1:]))
            elif cmd == 'call_function_in_opentenbase_test':
                self.CallFunctionInOpenTenBaseTest(linesplited[1])
            elif cmd == 'replace_then_send_wait_result':
                sql = ' '.join(i for i in linesplited[2:])
                sql = sql.replace(linesplited[1], self.kv_list[linesplited[1]])
                print(sql)
                self.ExecuteSqlOneLine(current_conn, sql + '\n')
            elif cmd == 'execute_shell_cmd':
                shell_cmd = line[len('execute_shell_cmd'):]
                os.system(shell_cmd)
            else:
                self.ExecuteSqlOneLine(current_conn, line_org)
        for conn in conns.values():
            if conn:
                conn.terminate()
                conn.wait()
        conns = None
        self.fout_case.flush()
        self.fout_case.close()

        ret = True
        if not global_args.record and os.path.exists(test.result_file):
            rc = ResultCompare(test.name, test.result_file, cast_output_file, 'sum.diffs', 'tmp')
            ret = rc.compare()
        if ret:
            ResultAgg(test.name)
            global_thl_data.logger.success(f'{test.name} success')
        else:
            global_thl_data.logger.error(f'{test.name} failed')

        end_time  = datetime.now()
        time_diff = end_time - start_time
        global_thl_data.logger.info(f'run test {test.name} use time: {time_diff.total_seconds()}')

    def receiveoutput(self, p):
        while True:
            line = p.stdout.readline().decode('utf8')
            if line == 'b64eab59-6926-45a0-8e5b-fe5ca5926002-d5186d32ca207466062889283c40ff96-stdout\n':
                break
            self.fout_case.write(line)
            if global_args.debug:
                print(line, end = '')
            if not line:
                if global_args.debug:
                    print('connection closed. read EOF on stdout')
                break
        while True:
            line = p.stderr.readline().decode('utf8')
            if line == 'b64eab59-6926-45a0-8e5b-fe5ca5926002-d5186d32ca207466062889283c40ff96-stderr\n':
                break
            self.fout_case.write(line)
            if global_args.debug:
                print(line, end = '')
            if not line:
                if global_args.debug:
                    print('connection closed. read EOF on stderr')
                break
        self.fout_case.flush()

    def ExecuteSqlOneLine(self, current_conn, line):
        current_conn.stdin.write((line).encode('utf8'))
        current_conn.stdin.flush()
        self.receiveoutput(current_conn)

    def SendSql(self, current_conn, line):
        current_conn.stdin.write((line).encode('utf8'))
        current_conn.stdin.flush()

    def SendBlockSql(self, current_conn, line):
        current_conn.stdin.write((line).encode('utf8'))
        current_conn.stdin.flush()
        time.sleep(1)
        line = current_conn.stdout.readline().decode('utf8')
        self.fout_case.write(line)
        rlist, _, _ = select.select([current_conn.stdout, current_conn.stderr], [], [], 0.1)
        if rlist:
            self.fout_case.write("the previous sql is not blocked")

    def CallFunctionInOpenTenBaseTest(self, name):
        if name == 'get_max_cpu_no':
            self.kv_list['max_cpu_no'] = str(os.cpu_count() - 1)
        elif name =='get_all_opentenbase_cpuset':
            file = "/sys/fs/cgroup/cpuset/opentenbase/cpuset.cpus"
            fd = open(file)
            line = fd.readline()
            fd.close()
            line = line.strip('\n')
            self.kv_list['all_opentenbase_cpuset'] = line
        else:
            assert False

class OpenTenBaseTestController:
    def Run(self):
        testcase_mgr = TestCaseMgr(g_test_dir, g_result_dir)
        testcase_mgr.GetAllTestCases(g_test_dir)
        testcase_mgr.PrintAllTestCases()
        testcase_mgr.GetAllUniqueClusterConf()
        self._test_case_list = testcase_mgr.test_case_list
        self._unique_conf = testcase_mgr.unique_conf
        self._skip_case = testcase_mgr._skip_case
        if not global_args.skip_create_base_cluster:
            os.system('[ -d basedata ] && rm -rf basedata')
            self.CreateBaseClusters()

        self.CreateAllTask()
        self.RunAllTask()
        self.PrintResult()

    def PrintResult(self):
        failed_test_set = set([testcase.name for testcase in self._test_case_list])
        failed_test_set -= global_passed_set
        global_thl_data.logger.info("=======================================================================")
        global_thl_data.logger.info("============                    Summary                    ============")
        global_thl_data.logger.info("============      passed/total  %4d/%-4d skipped %-3d      ============" \
                                    % (len(global_passed_set), len(self._test_case_list), self._skip_case))
        global_thl_data.logger.info("=======================================================================")
        
        if len(failed_test_set) :
            global_thl_data.logger.error("%d failed tests:" %len(failed_test_set))
            for v in failed_test_set:
                global_thl_data.logger.error(v)
        assert len(failed_test_set) + len(global_passed_set) == len(self._test_case_list), \
               "Why???????? This should not happen"

    def CreateAllTask(self):
        self.thread_task_list = []
        for test in self._test_case_list:
            task = ThreadTask(f'opentenbasetest-{test.name}')
            task.test = test
            self.thread_task_list.append(task)

    def RunAllTask(self):
        scheduler = Scheduler(global_args.parallel);
        for task in self.thread_task_list:
            scheduler.run_task(task)
        for task in self.thread_task_list:
            task.join()

    def CreateBaseClusters(self):
        start_time = datetime.now()
        for c in self._unique_conf:
            name = c.name()
            basedir = f'{global_base_dir}/basedata/{name}'
            CreateBaseCluster(c, basedir)
        end_time  = datetime.now()
        time_diff = end_time - start_time
        global_thl_data.logger.info(f'create unique cluster use time: {time_diff.total_seconds()}')

global_thl_data.logger.info('PreChecking...')
PreCheck.check()
controller = OpenTenBaseTestController()
controller.Run()
