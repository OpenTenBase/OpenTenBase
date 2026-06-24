#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
# Author: freemeng

import os
import copy
import threading
import time
import psycopg2
import pickle
import re
import struct
import getpass

from common import ClusterConf
from common import ExecCommand
from common import global_thl_data

if __name__ == "__main__":
    raise Exception("This script cannot be executed directly")

def ToBashArray(l):
    ret = '('
    for item in l:
        ret = ret + str(item) + ' '
    ret = ret + ')'
    return ret

global_base_port = 10000

def SetGlobalVar(baseport):
    global global_base_port
    global_base_port = baseport

_lock_for_ports = threading.Lock()
def AllocatePorts(num):
    with _lock_for_ports:
        global global_base_port
        ret = global_base_port
        global_base_port = global_base_port + num
    return ret


def ChangePortInPgclass(filename, new_port, new_forward_port, *args):
    src_tuple = struct.pack('i', args[0]) + \
                struct.pack('64s', args[1].encode('utf8').ljust(64, b'\0')) + \
                struct.pack('c', args[2].encode('utf8')) + \
                struct.pack('?', False) + \
                struct.pack('?', False) + \
                struct.pack('?', False) + \
                struct.pack('i', args[3]) + \
                struct.pack('i', args[4]) + \
                struct.pack('64s', args[5].encode('utf8').ljust(64, b'\0')) + \
                struct.pack('?', args[6]) + \
                struct.pack('?', args[7]) + \
                struct.pack('?', False) + \
                struct.pack('?', False) + \
                struct.pack('i', args[8]) + \
                struct.pack('64s', args[9].encode('utf8').ljust(64, b'\0')) + \
                struct.pack('i', args[10])

    dst_tuple = struct.pack('i', args[0]) + \
                struct.pack('64s', args[1].encode('utf8').ljust(64, b'\0')) + \
                struct.pack('c', args[2].encode('utf8')) + \
                struct.pack('?', False) + \
                struct.pack('?', False) + \
                struct.pack('?', False) + \
                struct.pack('i', new_port) + \
                struct.pack('i', new_forward_port) + \
                struct.pack('64s', args[5].encode('utf8').ljust(64, b'\0')) + \
                struct.pack('?', args[6]) + \
                struct.pack('?', args[7]) + \
                struct.pack('?', False) + \
                struct.pack('?', False) + \
                struct.pack('i', args[8]) + \
                struct.pack('64s', args[9].encode('utf8').ljust(64, b'\0')) + \
                struct.pack('i', args[10])

    with open(filename, 'rb') as f:
        content = f.read()
    pos = content.find(src_tuple)
    if pos < 0 :
        global_thl_data.logger.error(f"change pg_class port failed: file {filename}, {args[1]} from port {args[3]} forward port {args[4]} to new_port {new_port} new_forward_port {new_forward_port} ")
        assert False
    content = content[:pos] + dst_tuple + content[pos + len(dst_tuple):]
    with open(filename, 'wb') as f:
        succ_size = f.write(content)
        assert(succ_size == len(content))
    global_thl_data.logger.debug(f"change pg_class port success: file {filename}, {args[1]} from port {args[3]} forward port {args[4]} to new_port {new_port} new_forward_port {new_forward_port} ")


class PgxcCtlConf:
    def __init__(self, cluster_conf, basedir):
        self.cluster_conf = cluster_conf
        self.basedir = basedir

    def SetMemberVariables(self):
        assert self.cluster_conf
        assert self.basedir
        need_port_num = 0
        need_port_num = need_port_num + 2                                                     # gtm
        need_port_num = need_port_num + 3 * self.cluster_conf.cn                              # cn
        need_port_num = need_port_num + 2 * self.cluster_conf.cn * self.cluster_conf.cn_slave # slave cn
        need_port_num = need_port_num + 3 * self.cluster_conf.dn                              # dn
        need_port_num = need_port_num + 2 * self.cluster_conf.dn * self.cluster_conf.dn_slave # dn slave

        cur_port = AllocatePorts(need_port_num)
        global_thl_data.logger.debug(f'allocate port [{cur_port}, {cur_port + need_port_num})')
        self.ports = [ i for i in range(cur_port, cur_port + need_port_num)]

        self.pgxcOwner = getpass.getuser()
        self.rundir = self.basedir + '/database'
        self.gtmMasterPort = cur_port
        self.gtmSlavePort = cur_port + 1
        cur_port = cur_port + 2
        self.coordNames = []
        self.coordPorts = []
        self.coordForwardPorts = []
        self.poolerPorts = []
        self.coordMasterServers = []
        self.coordMasterDirs = []
        self.coordMaxWALSenders = []
        self.cn_pgxc_node = []
        self.cn_pgxc_node_file = []

        for i in range(self.cluster_conf.cn):
            self.coordNames.append(f'cn{i + 1}')
            self.coordPorts.append(cur_port)
            self.coordForwardPorts.append(cur_port + 1)
            self.poolerPorts.append(cur_port + 2)
            cur_port = cur_port + 3
            self.coordMasterServers.append('$myip')
            self.coordMasterDirs.append(f'$coordMasterDir/coord_cn{i + 1}')
            self.coordMaxWALSenders.append('$coordMaxWALsernder')

        self.coordSlaveServers = []
        self.coordSlavePorts = []
        self.coordSlavePoolerPorts = []
        self.coordSlaveDirs = []
        self.coordArchLogDirs = []
        self.cn_slave_pgxc_node = []
        self.cn_slave_pgxc_node_file = []

        self.coordSlave = 'y' if self.cluster_conf.cn_slave > 0 else 'n'
        if self.coordSlave == 'y':
            for i in range(self.cluster_conf.cn_slave):
                for i in range(self.cluster_conf.cn):
                    self.coordSlaveServers.append('$myip')
                    self.coordSlavePorts.append(cur_port)
                    self.coordSlavePoolerPorts.append(cur_port + 1)
                    cur_port = cur_port + 2
                    self.coordSlaveDirs.append(f'$coordSlaveDir/coord_cn{i + 1}')
                    self.coordArchLogDirs.append(f'$coordArchLogDir/coord_cn{i + 1}')

        self.datanodeNames = []
        self.datanodePorts = []
        self.datanodeForwardPorts = []
        self.datanodePoolerPorts = []
        self.datanodeMasterServers = []
        self.datanodeMasterDirs = []
        self.datanodeMaxWALSenders = []
        self.dn_pgxc_node = []
        self.dn_pgxc_node_file = []

        for i in range(self.cluster_conf.dn):
            self.datanodeNames.append(f'dn{i + 1}')
            self.datanodePorts.append(cur_port)
            self.datanodeForwardPorts.append(cur_port + 1)
            self.datanodePoolerPorts.append(cur_port + 2)
            cur_port = cur_port + 3
            self.datanodeMasterServers.append('$myip')
            self.datanodeMasterDirs.append(f'$dnMstrDir/dn{i + 1}')
            self.datanodeMaxWALSenders.append('$dnWALSndr')
        self.datanodeSlave = 'y' if self.cluster_conf.dn_slave > 0 else 'n'

        self.datanodeSlaveServers = []
        self.datanodeSlavePorts = []
        self.datanodeSlavePoolerPorts = []
        self.datanodeSlaveDirs = []
        self.datanodeArchLogDirs = []
        self.dn_slave_pgxc_node = []
        self.dn_slave_pgxc_node_file = []

        for i in range(self.cluster_conf.dn_slave):
            for i in range(self.cluster_conf.dn):
                self.datanodeSlaveServers.append('$myip')
                self.datanodeSlavePorts.append(cur_port)
                self.datanodeSlavePoolerPorts.append(cur_port + 1)
                cur_port = cur_port + 2
                self.datanodeSlaveDirs.append(f'$dnSlvDir/dn{i + 1}')
                self.datanodeArchLogDirs.append(f'$dnALDir/dn{i + 1}')

        assert cur_port == self.ports[0] + need_port_num

    def WriteToFile(self):
        content = open('pgxc_ctl.conf').read()
        content = content.replace('_TABSE_TEST_XXX_YYY_____pgxcOwner', self.pgxcOwner)
        content = content.replace('_TABSE_TEST_XXX_YYY_____rundir', self.rundir)
        content = content.replace('_TABSE_TEST_XXX_YYY_____gtmMasterPort', str(self.gtmMasterPort))
        content = content.replace('_TABSE_TEST_XXX_YYY_____gtmSlavePort', str(self.gtmSlavePort))
        content = content.replace('_TABSE_TEST_XXX_YYY_____gtmSlavePort', str(self.gtmSlavePort))
        content = content.replace('_TABSE_TEST_XXX_YYY_____coordNames', ToBashArray(self.coordNames))
        content = content.replace('_TABSE_TEST_XXX_YYY_____coordPorts', ToBashArray(self.coordPorts))
        content = content.replace('_TABSE_TEST_XXX_YYY_____coordForwardPorts', ToBashArray(self.coordForwardPorts))
        content = content.replace('_TABSE_TEST_XXX_YYY_____poolerPorts', ToBashArray(self.poolerPorts))
        content = content.replace('_TABSE_TEST_XXX_YYY_____coordMasterServers', ToBashArray(self.coordMasterServers))
        content = content.replace('_TABSE_TEST_XXX_YYY_____coordMasterDirs', ToBashArray(self.coordMasterDirs))
        content = content.replace('_TABSE_TEST_XXX_YYY_____coordMaxWALSenders', ToBashArray(self.coordMaxWALSenders))
        content = content.replace('_TABSE_TEST_XXX_YYY_____coordSlave__', self.coordSlave)
        content = content.replace('_TABSE_TEST_XXX_YYY_____coordSlaveServers', ToBashArray(self.coordSlaveServers))
        content = content.replace('_TABSE_TEST_XXX_YYY_____coordSlavePorts', ToBashArray(self.coordSlavePorts))
        content = content.replace('_TABSE_TEST_XXX_YYY_____coordSlavePoolerPorts', ToBashArray(self.coordSlavePoolerPorts))
        content = content.replace('_TABSE_TEST_XXX_YYY_____coordSlaveDirs', ToBashArray(self.coordSlaveDirs))
        content = content.replace('_TABSE_TEST_XXX_YYY_____coordArchLogDirs', ToBashArray(self.coordArchLogDirs))
        content = content.replace('_TABSE_TEST_XXX_YYY_____datanodeNames', ToBashArray(self.datanodeNames))
        content = content.replace('_TABSE_TEST_XXX_YYY_____datanodePorts', ToBashArray(self.datanodePorts))
        content = content.replace('_TABSE_TEST_XXX_YYY_____datanodeForwardPorts', ToBashArray(self.datanodeForwardPorts))
        content = content.replace('_TABSE_TEST_XXX_YYY_____datanodePoolerPorts', ToBashArray(self.datanodePoolerPorts))
        content = content.replace('_TABSE_TEST_XXX_YYY_____datanodeMasterServers', ToBashArray(self.datanodeMasterServers))
        content = content.replace('_TABSE_TEST_XXX_YYY_____datanodeMasterDirs', ToBashArray(self.datanodeMasterDirs))
        content = content.replace('_TABSE_TEST_XXX_YYY_____datanodeMaxWALSenders', ToBashArray(self.datanodeMaxWALSenders))
        content = content.replace('_TABSE_TEST_XXX_YYY_____datanodeMaxWALSenders', ToBashArray(self.datanodeMaxWALSenders))
        content = content.replace('_TABSE_TEST_XXX_YYY_____datanodeSlave__', self.datanodeSlave)
        content = content.replace('_TABSE_TEST_XXX_YYY_____datanodeSlaveServers', ToBashArray(self.datanodeSlaveServers))
        content = content.replace('_TABSE_TEST_XXX_YYY_____datanodeSlavePorts', ToBashArray(self.datanodeSlavePorts))
        content = content.replace('_TABSE_TEST_XXX_YYY_____datanodeSlavePoolerPorts', ToBashArray(self.datanodeSlavePoolerPorts))
        content = content.replace('_TABSE_TEST_XXX_YYY_____datanodeSlaveDirs', ToBashArray(self.datanodeSlaveDirs))
        content = content.replace('_TABSE_TEST_XXX_YYY_____datanodeArchLogDirs', ToBashArray(self.datanodeArchLogDirs))

        assert self.basedir
        assert self.rundir
        os.system(f'[ -d {self.basedir} ] || mkdir -p {self.basedir}')

        with open(f'{self.basedir}/pgxc_ctl.conf', 'w') as fout:
            fout.write(content)
        global_thl_data.logger.debug(f'{self.basedir}/pgxc_ctl.conf has been writen on disk')

    @staticmethod
    def GeneratePgxcCtlConfNoPortsInPgclass(cluster_conf, basedir):
        pgxc_ctl_conf = PgxcCtlConf(cluster_conf, basedir)

        pgxc_ctl_conf.SetMemberVariables()
        pgxc_ctl_conf.WriteToFile()

        return pgxc_ctl_conf

    @staticmethod
    def LoadFromPickleFile(basedir):
        with open(f"{basedir}/pgxc_ctl_conf.pickle", "rb") as fin_pickle:
            pgxc_ctl_conf = pickle.load(fin_pickle)
        return pgxc_ctl_conf

    def DumpToPickleFile(self):
        with open(f'{self.basedir}/pgxc_ctl_conf.pickle', 'wb') as fout_pickle:
            pickle.dump(self, fout_pickle)

    def GetNodePortInPgclass(self, nodeid):
        nodeid = re.sub(r'_', '', nodeid).lower()

        if nodeid == 'gtm':
            return self.gtmMasterPort

        matchObj = re.match(r'^cn(\d+)$', nodeid)
        if matchObj:
            cn = int(matchObj.group(1))
            return self.coordPorts[cn - 1]

        matchObj = re.match(r'^dn(\d+)$', nodeid)
        if matchObj:
            dn = int(matchObj.group(1))
            return self.datanodePorts[dn - 1]

        matchObj = re.match(r'^cn(\d+)slave(\d+)$', nodeid)
        if matchObj:
            cn = int(matchObj.group(1))
            slave = int(matchObj.group(2))
            assert slave == 1
            return self.coordSlavePorts[cn - 1]

        matchObj = re.match(r'^dn(\d+)slave(\d+)$', nodeid)
        if matchObj:
            dn = int(matchObj.group(1))
            slave = int(matchObj.group(2))
            assert slave == 1
            return self.datanodeSlavePorts[dn - 1]

        assert False


    def GetNodeForwardPortInPgclass(self, nodeid):
        nodeid = re.sub(r'_', '', nodeid).lower()

        if nodeid == 'gtm':
            return 0

        matchObj = re.match(r'^cn(\d+)$', nodeid)
        if matchObj:
            cn = int(matchObj.group(1))
            return self.coordForwardPorts[cn - 1]

        matchObj = re.match(r'^dn(\d+)$', nodeid)
        if matchObj:
            dn = int(matchObj.group(1))
            return self.datanodeForwardPorts[dn - 1]

        assert False

def CreateBaseCluster(cluster_conf, basedir):
    pgxc_ctl_conf = PgxcCtlConf.GeneratePgxcCtlConfNoPortsInPgclass(cluster_conf, basedir)

    cmd = f'pgxc_ctl --home {basedir} -c {basedir}/pgxc_ctl.conf init all >&log/{cluster_conf.name()}.log'
    global_thl_data.logger.info(cmd)
    os.system(cmd)
    time.sleep(5)

    user = getpass.getuser()

    for p in pgxc_ctl_conf.coordPorts:
        try:
            conn = psycopg2.connect(f"dbname='postgres' user='{user}' host='localhost' port={p}")
            curs =  conn.cursor()
            curs.execute("select oid, * from pgxc_node;")
            all_rows = curs.fetchall()
            pgxc_ctl_conf.cn_pgxc_node.append(all_rows)
            global_thl_data.logger.debug(f'cn pg_class content {all_rows}')
            curs.execute("select pg_relation_filepath('pgxc_node')")
            res = curs.fetchone()
            pgxc_ctl_conf.cn_pgxc_node_file.append(res)
            global_thl_data.logger.debug(f'cn pg_class file {res}')
        except:
            global_thl_data.logger.error("Get pgxc node failed")
    for p in pgxc_ctl_conf.coordSlavePorts:
        conn_string = f"dbname='postgres' user='{user}' host='localhost' port={p}"
        try:
            conn = psycopg2.connect(conn_string)
            curs =  conn.cursor()
            curs.execute("select oid, * from pgxc_node;")
            all_rows = curs.fetchall()
            pgxc_ctl_conf.cn_slave_pgxc_node.append(all_rows)
            global_thl_data.logger.debug(f'cn slave pg_class content {all_rows}')
            curs.execute("select pg_relation_filepath('pgxc_node')")
            res = curs.fetchone()
            pgxc_ctl_conf.cn_slave_pgxc_node_file.append(res)
            global_thl_data.logger.debug(f'cn slave pg_class file {res}')
        except:
            global_thl_data.logger.error(f"Get pgxc node failed: {conn_string}")
            assert False
    for p in pgxc_ctl_conf.datanodePorts:
        try:
            conn = psycopg2.connect(f"dbname='postgres' user='{user}' host='localhost' port={p}")
            curs =  conn.cursor()
            curs.execute("select oid, * from pgxc_node;")
            all_rows = curs.fetchall()
            pgxc_ctl_conf.dn_pgxc_node.append(all_rows)
            global_thl_data.logger.debug(f'dn pg_class content {all_rows}')
            curs.execute("select pg_relation_filepath('pgxc_node')")
            res = curs.fetchone()
            pgxc_ctl_conf.dn_pgxc_node_file.append(res)
            global_thl_data.logger.debug(f'dn pg_class file {res}')
        except:
            global_thl_data.logger.error("Get pgxc node failed")
    for p in pgxc_ctl_conf.datanodeSlavePorts:
        try:
            conn = psycopg2.connect(f"dbname='postgres' user='{user}' host='localhost' port={p}")
            curs =  conn.cursor()
            curs.execute("select oid, * from pgxc_node;")
            all_rows = curs.fetchall()
            pgxc_ctl_conf.dn_slave_pgxc_node.append(all_rows)
            global_thl_data.logger.debug(f'dn slave pg_class content {all_rows}')
            curs.execute("select pg_relation_filepath('pgxc_node')")
            res = curs.fetchone()
            pgxc_ctl_conf.dn_slave_pgxc_node_file.append(res)
            global_thl_data.logger.debug(f'dn slave pg_class file {res}')
        except:
            global_thl_data.logger.error("Get pgxc node failed")


    cmd = f'pgxc_ctl --home {basedir} -c {basedir}/pgxc_ctl.conf stop all >&/dev/null'
    os.system(cmd)
    time.sleep(2)

    pgxc_ctl_conf.DumpToPickleFile()


def ChangePortsInAllConfFiles(src_ports, dst_ports, dst_database_dir):
    if set(src_ports).intersection(set(dst_ports)):
        global_thl_data.logger.error('src_port and dst_ports can not overlap')
        assert False
    sedcmd = ';'.join(f's/port = {src_ports[i]}/port = {dst_ports[i]}/g; ' for i in range(len(src_ports)))

    cmd = f'find {dst_database_dir} -name "postgresql.conf" -o -name "recovery.conf" -o -name "gtm.conf" -o -name pgxc_ctl.conf '

    os.system('pwd')
    r = ExecCommand(cmd)
    for name in r.Stdout().split('\n'):
        name = name.strip()
        if not name:
            continue
        cmd = f"sed -i '{sedcmd}' {name}"
        global_thl_data.logger.debug(cmd)
        ExecCommand(cmd)


def CloneCluster(srcdir, targetdir):
    srcdir = os.path.normpath(srcdir)
    targetdir = os.path.normpath(targetdir)

    global_thl_data.logger.info(f'copy cluster from {srcdir} to {targetdir}')

    tp = os.path.dirname(targetdir)
    cmd = f'[ -d {tp} ] || mkdir {tp}'
    global_thl_data.logger.debug(cmd)
    os.system(cmd)

    cmd = f'[ -d {targetdir} ] && rm -rf {targetdir}'
    global_thl_data.logger.debug(cmd)
    os.system(cmd)

    cmd = f'cp -a {srcdir} {targetdir}'
    global_thl_data.logger.debug(cmd)
    os.system(cmd)

    src_pgxc_ctl_conf = PgxcCtlConf.LoadFromPickleFile(targetdir)
    dst_pgxc_ctl_conf = copy.deepcopy(src_pgxc_ctl_conf)
    dst_pgxc_ctl_conf.basedir = targetdir
    dst_pgxc_ctl_conf.SetMemberVariables()
    dst_pgxc_ctl_conf.WriteToFile()
    global_thl_data.logger.debug('pgxc_ctl.conf with new ports has been writen on disk')
    # useless and ports in pgclass not changed
    dst_pgxc_ctl_conf.DumpToPickleFile()

    src_ports = src_pgxc_ctl_conf.ports
    dst_ports = dst_pgxc_ctl_conf.ports
    global_thl_data.logger.debug(f'change ports in configure files from {src_ports} to {dst_ports}')
    ChangePortsInAllConfFiles(src_ports, dst_ports, dst_pgxc_ctl_conf.rundir)

    assert len(src_pgxc_ctl_conf.coordMasterDirs) == len(dst_pgxc_ctl_conf.coordMasterDirs)
    assert len(src_pgxc_ctl_conf.coordSlaveDirs) == len(dst_pgxc_ctl_conf.coordSlaveDirs)
    assert len(src_pgxc_ctl_conf.datanodeMasterDirs) == len(dst_pgxc_ctl_conf.datanodeMasterDirs)
    assert len(src_pgxc_ctl_conf.datanodeSlaveDirs) == len(dst_pgxc_ctl_conf.datanodeSlaveDirs)

    cn_count = len(src_pgxc_ctl_conf.coordMasterDirs)
    cn_slave_count = len(src_pgxc_ctl_conf.coordSlaveDirs)
    dn_count = len(src_pgxc_ctl_conf.datanodeMasterDirs)
    dn_slave_count = len(src_pgxc_ctl_conf.datanodeSlaveDirs)

    global_thl_data.logger.debug('changing ports in pg_class file')
    mydir = targetdir + '/database'
    for i in range(cn_count):
        assert len(dst_pgxc_ctl_conf.cn_pgxc_node_file) == 0
        assert len(dst_pgxc_ctl_conf.cn_pgxc_node) == 0
        relfile = f'{mydir}/coord/coord_cn{i+1}' + '/' + src_pgxc_ctl_conf.cn_pgxc_node_file[i][0]
        for node in src_pgxc_ctl_conf.cn_pgxc_node[i]:
            ChangePortInPgclass(relfile, dst_pgxc_ctl_conf.GetNodePortInPgclass(node[1]), dst_pgxc_ctl_conf.GetNodeForwardPortInPgclass(node[1]), *node)

    for i in range(cn_slave_count):
        assert len(dst_pgxc_ctl_conf.cn_slave_pgxc_node_file) == 0
        assert len(dst_pgxc_ctl_conf.cn_slave_pgxc_node) == 0
        relfile = f'{mydir}/coord_slave/coord_cn{i+1}' + '/' + src_pgxc_ctl_conf.cn_slave_pgxc_node_file[i][0]
        for node in src_pgxc_ctl_conf.cn_slave_pgxc_node[i]:
            ChangePortInPgclass(relfile, dst_pgxc_ctl_conf.GetNodePortInPgclass(node[1]), dst_pgxc_ctl_conf.GetNodeForwardPortInPgclass(node[1]), *node)

    for i in range(dn_count):
        assert len(dst_pgxc_ctl_conf.dn_pgxc_node_file) == 0
        assert len(dst_pgxc_ctl_conf.dn_pgxc_node) == 0
        relfile = f'{mydir}/dn_master/dn{i+1}' + '/' + src_pgxc_ctl_conf.dn_pgxc_node_file[i][0]
        for node in src_pgxc_ctl_conf.dn_pgxc_node[i]:
            ChangePortInPgclass(relfile, dst_pgxc_ctl_conf.GetNodePortInPgclass(node[1]), dst_pgxc_ctl_conf.GetNodeForwardPortInPgclass(node[1]), *node)

    for i in range(dn_slave_count):
        assert len(dst_pgxc_ctl_conf.dn_slave_pgxc_node_file) == 0
        assert len(dst_pgxc_ctl_conf.dn_slave_pgxc_node) == 0
        relfile = f'{mydir}/dn_slave/dn{i+1}' + '/' + src_pgxc_ctl_conf.dn_slave_pgxc_node_file[i][0]
        for node in src_pgxc_ctl_conf.dn_slave_pgxc_node[i]:
            ChangePortInPgclass(relfile, dst_pgxc_ctl_conf.GetNodePortInPgclass(node[1]), dst_pgxc_ctl_conf.GetNodeForwardPortInPgclass(node[1]), *node)

    global_thl_data.logger.debug('Changed ports in pg_class file. done')

def ChangeConf(dst_dir, conf_item):
    node_parent_dirs = ['coord', 'coord_slave', 'dn_master', 'dn_slave']
    for parent in node_parent_dirs:
        onetypename = dst_dir + '/database/' + parent
        onetypedir = os.listdir(onetypename)
        for pgdata in onetypedir:
            name = pgdata + '/postgresql.conf'
            conf_key = conf_item.split('=')[0]
            cmd = f"sed -i '/{conf_key}/d' {name}"
            global_thl_data.logger.debug(cmd)
            ExecCommand(cmd)
            cmd = f'echo -e "\\nadded by opentenbase_test test case\\n{conf_item}" >> {name}'
            global_thl_data.logger.debug(cmd)
            ExecCommand(cmd)
    for parent in node_parent_dirs:
        onetypename = dst_dir + '/database/' + parent
        onetypedir = os.listdir(onetypename)
        for pgdata in onetypedir:
            cmd = f'pg_ctl -D {pgdata} reload'
            global_thl_data.logger.debug(cmd)
            ExecCommand(cmd)
