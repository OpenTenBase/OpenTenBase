#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
# Author: freemeng

if __name__ == "__main__":
    raise Exception("This script cannot be executed directly")



import threading
import sys
from loguru import logger
from subprocess import PIPE, Popen


_global_lock = threading.Lock()
global_thl_data = threading.local()

class OpenTenBaseLogger():
    @staticmethod
    def CreateLogggerForThisThread(log_file):
        global_thl_data.logger = OpenTenBaseLogger(log_file)
    def __init__(self, log_file):
        with _global_lock:
            if threading.current_thread().name == 'MainThread':
                desc = ''
                logger.remove(handler_id=0)
                logger.add(sys.stdout, format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
                           "<level>{level: <8}</level> |  <level>{extra[thread_desc]}{message}</level>", enqueue=True,
                           level="INFO", colorize=True, backtrace=True, diagnose=True)
                logger.add(log_file, format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {file}:{function}():{line} - {extra[thread_desc]}{message}",
                       level="DEBUG", encoding="UTF-8", filter=lambda record: log_file == record["extra"].get("basefilename") or \
                       record["level"].no >= 20,
                       enqueue=True, rotation="200 MB", retention="1 month", backtrace=True, diagnose=True)
            else:
                self.loggerid = logger.add(log_file, format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {file}:{function}():{line} - {message}",
                       level="DEBUG", encoding="UTF-8", filter=lambda record: log_file == record["extra"].get("basefilename"),
                       enqueue=True, rotation="200 MB", retention="1 month", backtrace=True, diagnose=True)
                desc = threading.current_thread().name + ' '

            self.logger = logger.bind(basefilename=log_file, thread_desc = desc)
            global_thl_data.logger = self

    def close(self):
        logger.remove(self.loggerid)

    def debug(self, msg, *args, **kwargs):
        self.logger.opt(depth=1).debug(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self.logger.opt(depth=1).info(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self.logger.opt(depth=1).warning(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self.logger.opt(depth=1).error(msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        self.logger.opt(depth=1).critical(msg, *args, **kwargs)

    def success(self, msg, *args, **kwargs):
        self.logger.opt(depth=1).success(msg, *args, **kwargs)


class CommandResult:
    def __init__(self, stdout, stderr, exceptstr, returncode):
        self.stdout = ''
        self.stderr = ''
        if stdout:
            self.stdout = stdout.decode('utf8')
        if stderr:
            self.stderr = stderr.decode('utf8')
        self.exceptstr = exceptstr
        self.returncode = returncode

    def IsSuccess(self):
        return self.exceptstr == '' and self.returncode == 0

    def __str__(self):
        return '<stdout>:"%s" <stderr>:"%s" <returncode>:%d <exceptstr>:"%s"' % (
                self.stdout, self.stderr, self.returncode, self.exceptstr)
    def Stdout(self):
        if self.IsSuccess():
            return self.stdout
        else:
            assert False

def ExecCommand(cmd):
    try:
        if hasattr(global_thl_data, 'logger'):
            global_thl_data.logger.debug(cmd)
        p = Popen(args=cmd, stdout=PIPE, stderr=PIPE, shell=True)
        stdout, stderr = p.communicate()
        r = CommandResult(stdout, stderr, "", p.wait())
        if hasattr(global_thl_data, 'logger'):
            global_thl_data.logger.debug(r)
        return r
    except Exception as e:
        r = CommandResult("", "", str(e), -1)
        if hasattr(global_thl_data, 'logger'):
            global_thl_data.logger.debug(r)
        return r

class ClusterConf:
    __slots__ = ['cn', 'dn', 'gtm', 'cn_slave', 'dn_slave', 'gtm_slave']

    def __init__(self, *kwargs):
        for i, n in enumerate(self.__slots__):
            setattr(self, n, kwargs[i])

    def __str__(self):
        content =  ', '.join(f'{n} {getattr(self, n)}' for n in self.__slots__)
        return f'[ {content} ]'
    def __eq__(self, other):
        if not isinstance(other, ClusterConf):
            return False
        return all(getattr(self, n) == getattr(other, n) for n in self.__slots__)

    def __hash__(self):
        return hash(tuple(getattr(self, n) for n in self.__slots__))

    def name(self):
        return '_'.join(f'{n}_{getattr(self, n)}' for n in self.__slots__)
