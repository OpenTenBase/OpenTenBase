
if __name__ == "__main__":
    raise Exception("This script cannot be executed directly")


import os
import re

from common import global_thl_data
from common import ClusterConf

global_base_dir = ''
def SetGlobalVar(gdir):
    global global_base_dir
    global_base_dir = gdir

class TestCase:
    __slots__ = ['name', 'test_file', 'result_file', 'cluster_conf']
    def __init__(self, *kwargs):
        for i, n in enumerate(self.__slots__):
            setattr(self, n, kwargs[i])

        # just for check
        tname, text = os.path.splitext(os.path.basename(self.test_file))
        rname, rext = os.path.splitext(os.path.basename(self.result_file))
        assert tname == rname, "testname in '%s' and '%s' are not the same" % (self.test_file, self.result_file)
        assert text == ".test", "testfile '%s' should be end with '.test'" % (self.test_file)
        assert rext == ".result", "resultfile '%s' should be end with '.result'" % (self.result_file)

    def __str__(self):
        content =  ' '.join(f'{getattr(self, n)}' for n in self.__slots__)
        return f'[ {content} ]'

    def __lt__(self, other):
        return (self.test_file) \
             < (other.test_file)

class TestCaseMgr:
    def __init__(self, test_dir, result_dir):
        self.test_case_list = []
        self.unique_conf = set()
        self._skip_case = 0
        self._test_dir = test_dir
        self._result_dir = result_dir

    def DetermineTestCaseEnvironment(self, path):
        name_list = ['cn', 'dn', 'gtm', 'cn_slave', 'dn_slave', 'gtm_slave']
        count_list = [1, 1, 1, 0, 0, 0]
        f = open(path)
        for line in f:
            line = line.split("#")[0].strip()
            if not line or line[0] != '%':
                continue

            # TODO: check all % is at the beginning

            line = re.sub(r'\s+', '', line)

            if re.match(r'^\%[_a-zA-Z]+[\d]+$', line):
                alpha_part = re.findall(r'[_a-zA-Z]+', line)[0]
                numeric_part = re.findall(r'\d+', line)[0]
            else:
                global_thl_data.logger.debug(line)
                assert False

            node_count = int(numeric_part)

            assert len(name_list) == len(count_list)
            found = False
            for idx in range(len(name_list)):
                if name_list[idx] == alpha_part:
                    count_list[idx] = node_count
                    found = True
            assert found, f"{alpha_part} is not a legal configure word. It must in {name_list}"

        return ClusterConf(*count_list)

    def GetAllTestCases(self, path):
        assert os.path.islink(path) == False, "Do not use symbolic links: %s" % (path)
        if os.path.isfile(path):
            basename = os.path.basename(path)
            testname, ext = os.path.splitext(basename)
            if ext == ".conf":
                return
            if ext != ".test":
                self._skip_case += 1
                global_thl_data.logger.warning("skip %s" % path)
                return
            assert path.startswith(self._test_dir)
            if os.path.isdir(self._test_dir):
                if os.path.isdir(self._result_dir):
                    result_filepath = self._result_dir + \
                        path[len(self._test_dir): -len(basename)] + \
                        testname + ".result"
                else:
                    # this branch is very radiculious, we do not think you should us it like this
                    assert os.path.isfile(self._result_dir)
                    result_filepath = self._result_dir
            else:
                # single test case
                if os.path.isfile(self._result_dir):
                    result_filepath = self._result_dir
                else:
                    assert path == self._test_dir, "Internal error"
                    default_test_dir = os.path.realpath(global_base_dir + '/t/')
                    assert path.startswith(default_test_dir), "Single test case must be in the default location"
                    result_filepath = global_base_dir + '/r/' + \
                                    path[len(default_test_dir): -len(basename)] + \
                                    testname + ".result"
                    global_thl_data.logger.warning("use %s as the expected file(result_dir)" %result_filepath)

            cluster_conf = self.DetermineTestCaseEnvironment(path)
            self.test_case_list.append(TestCase(testname, path, result_filepath, cluster_conf))
        else:
            assert os.path.isdir(path)
            for tmpname in os.listdir(path):
                tmppath = os.path.join(path, tmpname)
                self.GetAllTestCases(tmppath)
        self.test_case_list.sort()

    def GetAllUniqueClusterConf(self):
        for test in self.test_case_list:
            self.unique_conf.add(test.cluster_conf)

    def PrintAllTestCases(self):
        for t in self.test_case_list:
            pass
