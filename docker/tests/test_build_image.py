#!/usr/bin/env python3
# Copyright (c) 2026 OpenTenBase contributors
# SPDX-License-Identifier: BSD-3-Clause

"""Test buildImage.sh's command orchestration without a Docker daemon.

Run with: python3 docker/tests/test_build_image.py
The production script is copied into a temporary checkout; only the Docker
CLI is replaced to record build contexts and inject build failures.
"""

import json
import os
import pathlib
import shutil
import subprocess
import tempfile
import unittest


SCRIPT = pathlib.Path(__file__).resolve().parents[1] / "buildImage.sh"
BASE_TAG = "opentenbasebase:1.0.0"
HOST_TAG = "opentenbase:1.0.0"


class BuildImageTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="docker-build-")
        self.addCleanup(self.temporary.cleanup)
        self.work = pathlib.Path(self.temporary.name)
        self.docker_dir = self.work / "checkout with spaces" / "docker"
        for directory in ("base", "host"):
            (self.docker_dir / directory).mkdir(parents=True)
        shutil.copyfile(SCRIPT, self.docker_dir / "buildImage.sh")
        self.bin = self.work / "bin"
        self.bin.mkdir()
        docker = self.bin / "docker"
        docker.write_text(
            "#!/usr/bin/env python3\n"
            "import json, os, sys\n"
            "with open(os.environ['BUILD_CALLS'], 'a') as log:\n"
            "    log.write(json.dumps([os.getcwd(), sys.argv[1:]]) + '\\n')\n"
            "sys.exit(37 if os.environ.get('FAIL_TAG') in sys.argv[1:] else 0)\n")
        docker.chmod(0o755)
        self.log = self.work / "calls.jsonl"

    def run_build(self, cwd, fail_tag=""):
        env = dict(os.environ, PATH=str(self.bin) + os.pathsep + os.environ["PATH"],
                   BUILD_CALLS=str(self.log), FAIL_TAG=fail_tag)
        result = subprocess.run(
            ["bash", str(self.docker_dir / "buildImage.sh")], cwd=cwd,
            env=env, text=True, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, check=False)
        calls = [json.loads(line) for line in self.log.read_text().splitlines()]
        return result, calls

    def assert_build_contexts(self, calls):
        self.assertEqual(len(calls), 2)
        for (cwd, argv), directory, tag in zip(
                calls, ("base", "host"), (BASE_TAG, HOST_TAG)):
            self.assertEqual(argv[:-1], ["build", "-t", tag])
            self.assertEqual((pathlib.Path(cwd) / argv[-1]).resolve(),
                             self.docker_dir / directory)

    def test_success_preserves_tags_and_contexts(self):
        result, calls = self.run_build(self.docker_dir)
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assert_build_contexts(calls)

    def test_can_run_outside_docker_directory(self):
        result, calls = self.run_build(self.work)
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assert_build_contexts(calls)

    def test_base_failure_stops_before_host_build(self):
        result, calls = self.run_build(self.docker_dir, BASE_TAG)
        self.assertEqual(result.returncode, 37, result.stdout)
        self.assertEqual(len(calls), 1)
        self.assertIn(BASE_TAG, calls[0][1])

    def test_host_failure_is_returned(self):
        result, calls = self.run_build(self.docker_dir, HOST_TAG)
        self.assertEqual(result.returncode, 37, result.stdout)
        self.assert_build_contexts(calls)


if __name__ == "__main__":
    unittest.main()
