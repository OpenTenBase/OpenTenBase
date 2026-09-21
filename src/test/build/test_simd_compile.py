#!/usr/bin/env python3
# Copyright (c) 2026 OpenTenBase contributors
# SPDX-License-Identifier: BSD-3-Clause

"""Exercise the production compile rule without configuring a database build.

Run with: python3 src/test/build/test_simd_compile.py
Only GNU make, GCC and the existing vectorization checker are needed.  The
empty Makefile.port omits platform flags; command-line variables replace the
configure substitutions needed by these standalone C compilation tests.
"""

import pathlib
import subprocess
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[3]


class SimdCompileTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="simd-compile-")
        self.addCleanup(self.temporary.cleanup)
        self.work = pathlib.Path(self.temporary.name)
        (self.work / "src").mkdir()
        (self.work / "src/Makefile.port").touch()

    def compile(self, source, simd="yes"):
        (self.work / "sample.c").write_text(source)
        return subprocess.run(
            ["make", "--no-print-directory", "-f",
             str(ROOT / "src/Makefile.global.in"), "sample.o",
             "SHELL=/bin/sh", "CC=gcc", "CFLAGS=", "CPPFLAGS=",
             "ICU_CFLAGS=", "enable_simd=" + simd, "enable_nls=no",
             "top_builddir=" + str(self.work), "top_srcdir=" + str(ROOT),
             "subdir=."],
            cwd=self.work, text=True, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, check=False)

    def test_simd_compiler_failure_is_not_hidden_by_logging(self):
        result = self.compile("#error intentional_compiler_failure\n")
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertFalse((self.work / "sample.o").exists())
        self.assertIn("intentional_compiler_failure", result.stdout)
        self.assertIn("intentional_compiler_failure",
                      (self.work / "sample.log").read_text())

    def test_success_keeps_compiler_diagnostics_and_log(self):
        result = self.compile(
            "#warning visible_compiler_warning\n"
            "int answer(void) { return 42; }\n")
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertTrue((self.work / "sample.o").exists())
        self.assertIn("visible_compiler_warning", result.stdout)
        self.assertIn("visible_compiler_warning",
                      (self.work / "sample.log").read_text())

    def test_vectorization_check_still_rejects_unvectorized_loop(self):
        result = self.compile(
            "int sum(int *a) { int result = 0;\n"
            "#pragma GCC ivdep\n"
            "for (int i = 0; i < 10; i++) result += a[i];\n"
            "return result; }\n")
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("failed to vectorized", result.stdout)
        self.assertFalse((self.work / "sample.o").exists())

    def test_non_simd_compiler_failure_remains_an_error(self):
        result = self.compile("#error intentional_compiler_failure\n", simd="no")
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertFalse((self.work / "sample.o").exists())


if __name__ == "__main__":
    unittest.main()
