#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
# Author: freemeng

if __name__ == "__main__":
    raise Exception("This script cannot be executed directly")

import argparse

class OpenTenBaseTestArgsParser:
    @staticmethod
    def parse():
        parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)

        parser.add_argument("--parallel", "-p", "-j", dest="parallel",
                            help=("how many parallel threads do you want?.\n"),
                            type=int,
                            default=1)
        parser.add_argument("-s", dest="skip_create_base_cluster", action='store_true',
                            help=("Whether to skip create base cluster\n"))
        parser.add_argument("--backup", "-b", dest="backup", action='store_true',
                            help=("Whether to backup the data and logs of passed test cases\n"))
        parser.add_argument("--test_dir", "-t", dest='test_dir',
                            help="set test case files directory or specify a single test file.\n",
                            type=str,
                            default='./t')
        parser.add_argument("--result_dir", "-r", "-e", dest='result_dir',
                            help="set result files directory or specify a single test file.\n",
                            type=str,
                            default='./r')
        parser.add_argument("--record", dest="record", action='store_true',
                            help=("Whether to record the result to a result file\n"))
        parser.add_argument("--debug", dest="debug", action='store_true',
                            help=("debug mode. print commands and outputs.\n"))

        args = parser.parse_args()
        return args
