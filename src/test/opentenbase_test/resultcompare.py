if __name__ == "__main__":
    raise Exception("This script cannot be executed directly")

import os
import re
from common import ExecCommand

class ResultCompare:
    def __init__(self, name, expect_file, result_file, sum_diff, tmp_dir):
        self.name = name
        self.expect_file = expect_file
        self.result_file = result_file
        self.sum_diff = sum_diff
        self.tmp_dir = tmp_dir

    def simple_diff(self, e, r):
        diff_file = f'{self.tmp_dir}/{self.name}.diff'
        cmd = f'diff --unified {e} {r} > {diff_file}'
        res = ExecCommand(cmd)
        if res.returncode == 0:
            os.remove(diff_file)
            return True
        return False

    def compare(self):
        assert os.path.exists(self.expect_file)
        assert os.path.exists(self.result_file)
        assert os.path.isdir(self.tmp_dir)
        if self.simple_diff(self.expect_file, self.result_file):
            return True

        self.process_expect()

        if self.has_reg_matched and self.simple_diff(self.new_expect_file, self.result_file):
            return True

        os.system(f'cat {self.tmp_dir}/{self.name}.diff >> {self.sum_diff}')
        return False

    def process_expect(self):
        self.new_expect_file = f'{self.tmp_dir}/{self.name}.expect'
        fout = open(self.new_expect_file, 'w')
        expect_content = open(self.expect_file).read().splitlines(True)
        result_content = open(self.result_file).read().splitlines(True)
        self.has_reg_matched = False
        for expect, result in zip(expect_content, result_content):
            this_line_matched = False
            if expect.startswith('--?'):
                pattern = re.compile('^' + expect[3:])

                if pattern.match(result):
                    this_line_matched = True
                    self.has_reg_matched = True

            if this_line_matched:
                fout.write(result)
            else:
                fout.write(expect)
        
        fout.flush()
        fout.close()

        if not self.has_reg_matched:
            os.remove(self.new_expect_file)
