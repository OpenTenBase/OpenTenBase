# Copyright (c) 2026 OpenTenBase Authors
# Licensed under the BSD 3-Clause License. See LICENSE.txt in the repository root.

"""Black-box tests for the build wrapper, using Bash and a simulated make.

Run with Python 3.10+ and Bash. Set BASH to the Bash executable when necessary.
Set COMPILE_SCRIPT to another revision of the wrapper for a comparison run.
Only temporary fixtures are modified. No compiler or database is invoked.
"""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


SCRIPT = Path(os.environ.get(
    "COMPILE_SCRIPT",
    Path(__file__).resolve().parents[1]
    / "opentenbase_ora_package_function_compile.sh",
)).resolve()
BASH = os.environ.get("BASH") or shutil.which("bash")

MOCK_MAKE = r'''#!/usr/bin/env bash
set -eu
expected_dir=$(cd -- "$MOCK_EXTENSION" && pwd)
if [[ "$PWD" != "$expected_dir" ]]; then
    echo "make called in unexpected directory: $PWD" >&2
    exit 90
fi
stage=build
for arg in "$@"; do
    case "$arg" in
        clean) stage=clean ;;
        install) stage=install ;;
    esac
done
printf '%s\n' "$stage" >> "$MOCK_LOG"
printf '%s\n' "$@" >> "$MOCK_ARGS"
if [[ "$stage" == "$MOCK_FAIL_STAGE" ]]; then
    echo "simulated $stage failure" >&2
    exit "$MOCK_FAIL_STATUS"
fi
'''

MOCK_GETCONF = r'''#!/usr/bin/env bash
printf '%s' "$MOCK_CPU_OUTPUT"
exit "$MOCK_CPU_STATUS"
'''


@unittest.skipUnless(BASH, "Bash is required")
class CompileScriptTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="otb-compile-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.tree = self.root / "source tree"
        self.extension = self.tree / "contrib/opentenbase_ora_package_function"
        self.extension.mkdir(parents=True)
        self.script = self.extension / SCRIPT.name
        # Normalize Git's Windows checkout line endings for execution under Bash.
        self.script.write_text(SCRIPT.read_text(encoding="utf-8"), encoding="utf-8",
                               newline="\n")
        self.config = self.tree / "src/Makefile.global"
        self.config.parent.mkdir()
        self.configure("'--prefix=/opt/otb'")
        self.bin = self.root / "mock-bin"
        self.bin.mkdir()
        for name, content in (("make", MOCK_MAKE), ("getconf", MOCK_GETCONF)):
            command = self.bin / name
            command.write_text(content, encoding="utf-8", newline="\n")
            command.chmod(0o755)
        self.log = self.root / "calls.log"
        self.args = self.root / "args.log"
        self.environment = os.environ.copy()
        self.environment.update({
            "MOCK_BIN": self.bin.as_posix(),
            "MOCK_EXTENSION": self.extension.as_posix(),
            "MOCK_LOG": self.log.as_posix(),
            "MOCK_ARGS": self.args.as_posix(),
            "MOCK_FAIL_STAGE": "",
            "MOCK_FAIL_STATUS": "7",
            "MOCK_CPU_OUTPUT": "4\n",
            "MOCK_CPU_STATUS": "0",
            "BASH_ENV": "",
            "ENV": "",
        })
        self.environment.pop("SHELLOPTS", None)
        self.environment.pop("BASHOPTS", None)
        self.environment.pop("CDPATH", None)

    def configure(self, arguments, prefix="/opt/otb",
                  exec_prefix="${prefix}", bindir="${exec_prefix}/bin"):
        self.config.write_text(
            "configure_args = " + arguments + "\n"
            "prefix := " + prefix + "\n"
            "exec_prefix := " + exec_prefix + "\n"
            "bindir := " + bindir + "\n",
            encoding="utf-8", newline="\n",
        )

    def run_script(self, cwd=None):
        # Resolve the mock directory inside Bash so Windows drive letters cannot
        # become PATH separators. The fixture is never placed on the host PATH.
        return subprocess.run(
            [BASH, "--noprofile", "--norc", "-c",
             'export PATH="$(cd -- "$MOCK_BIN" && pwd):$PATH"; '
             'exec bash "$1"', "test-compile", self.script.as_posix()],
            cwd=cwd or self.root, env=self.environment, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15,
        )

    def calls(self):
        return self.log.read_text().splitlines() if self.log.exists() else []

    def assert_success(self, result):
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.calls(), ["clean", "build", "install"])
        self.assertIn("compile is success", result.stdout)

    def test_explicit_prefix_from_unrelated_directory(self):
        self.assert_success(self.run_script())

    def test_invocation_from_repository_root(self):
        self.assert_success(self.run_script(self.tree))

    def test_invocation_from_contrib(self):
        self.assert_success(self.run_script(self.tree / "contrib"))

    def test_default_prefix_without_explicit_option(self):
        self.configure("'--without-readline'", prefix="/usr/local/pgsql")
        self.assert_success(self.run_script())

    def test_prefix_with_separate_value(self):
        self.configure("'--prefix' '/opt/otb'")
        self.assert_success(self.run_script())

    def test_configured_directories_are_left_to_make(self):
        self.configure("'--prefix=/opt/otb' '--exec-prefix=/opt/runtime' "
                       "'--bindir=/opt/client tools/bin'",
                       exec_prefix="/opt/runtime", bindir="/opt/client tools/bin")
        self.assert_success(self.run_script())
        self.assertEqual(self.args.read_text().splitlines(),
                         ["clean", "-j4", "install", "-j4"])

    def test_quoted_configure_arguments_are_not_reparsed(self):
        self.configure("'--prefix=/opt/otb tools' 'CFLAGS=-O2 -g'",
                       prefix="/opt/otb tools")
        self.assert_success(self.run_script())
        self.assertNotIn("PG_CONFIG=", self.args.read_text())

    def test_getconf_failure_falls_back_to_one_job(self):
        self.environment["MOCK_CPU_STATUS"] = "1"
        self.environment["MOCK_CPU_OUTPUT"] = ""
        self.assert_success(self.run_script())
        self.assertIn("-j1", self.args.read_text().splitlines())

    def test_zero_cpu_count_is_bounded(self):
        self.environment["MOCK_CPU_OUTPUT"] = "0\n"
        self.assert_success(self.run_script())
        self.assertIn("-j1", self.args.read_text().splitlines())

    def test_empty_cpu_count_is_bounded(self):
        self.environment["MOCK_CPU_OUTPUT"] = ""
        self.assert_success(self.run_script())
        self.assertIn("-j1", self.args.read_text().splitlines())

    def test_invalid_cpu_count_is_bounded(self):
        self.environment["MOCK_CPU_OUTPUT"] = "unknown\n"
        self.assert_success(self.run_script())
        self.assertIn("-j1", self.args.read_text().splitlines())

    def test_missing_configuration_stops_before_make(self):
        self.config.unlink()
        result = self.run_script()
        self.assertEqual(result.returncode, 1)
        self.assertEqual(self.calls(), [])
        self.assertIn("Run configure", result.stderr)
        self.assertNotIn("compile is success", result.stdout)

    def assert_stage_failure(self, stage, status, calls):
        self.environment["MOCK_FAIL_STAGE"] = stage
        self.environment["MOCK_FAIL_STATUS"] = str(status)
        result = self.run_script()
        self.assertEqual(result.returncode, status, result.stdout + result.stderr)
        self.assertEqual(self.calls(), calls)
        self.assertIn("error(" + str(status) + ")", result.stderr)
        self.assertNotIn("compile is success", result.stdout)

    def test_clean_failure_preserves_status_and_stops(self):
        self.assert_stage_failure("clean", 7, ["clean"])

    def test_build_failure_preserves_status_and_stops(self):
        self.assert_stage_failure("build", 13, ["clean", "build"])

    def test_install_failure_preserves_status_and_stops(self):
        self.assert_stage_failure("install", 23, ["clean", "build", "install"])

    def test_cdpath_does_not_change_script_directory(self):
        self.environment["CDPATH"] = self.root.as_posix()
        self.assert_success(self.run_script())


if __name__ == "__main__":
    unittest.main(verbosity=2)
