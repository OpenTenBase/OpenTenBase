#!/usr/bin/env python3
# Copyright (c) 2026, OpenTenBase Authors
# Licensed under the BSD-3-Clause license; see LICENSE.txt.

"""Check a real Linux GTM's startup affinity and timestamp service.

Run `make -C src/gtm check-affinity` in a configured build, or compare binaries:
  python3 test_affinity.py --gtm /path/to/gtm --probe /path/to/affinity_probe

Each case starts its own GTM with default worker settings. No database server,
initgtm, root privileges, cgroup changes, or third-party Python packages are
required. This tests inherited affinity (like taskset), not CPU-quota behavior.
Unavailable CPU topologies are explicitly skipped. Logs are kept; temporary
GTM data and only the child processes started here are cleaned up.
The standby case checks its early status service with an unreachable primary;
it does not exercise replication or failover.
"""

import argparse
import datetime
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time


def parse_cpu_list(value):
    cpus = set()
    for part in value.split(","):
        bounds = part.split("-")
        first = int(bounds[0])
        last = int(bounds[-1])
        cpus.update(range(first, last + 1))
    return cpus


def thread_masks(pid):
    threads = []
    for task in sorted(Path("/proc", str(pid), "task").iterdir()):
        try:
            fields = dict(line.split(":", 1) for line in
                          (task / "status").read_text().splitlines())
        except FileNotFoundError:
            continue
        threads.append({"tid": int(task.name), "name": fields["Name"].strip(),
                        "cpus": sorted(parse_cpu_list(fields["Cpus_allowed_list"]))})
    return threads


def expected_masks(cpus):
    ordered = sorted(cpus)
    timekeeper = {ordered[1] if len(ordered) > 1 else ordered[0]}
    services = set(ordered[2:]) if len(ordered) > 2 else {ordered[0]}
    return timekeeper, services


def check_masks(pid, cpus, threads):
    timekeeper, services = expected_masks(cpus)
    if len(threads) < 3:
        raise AssertionError("GTM has too few live threads: %r" % threads)
    for thread in threads:
        actual = set(thread["cpus"])
        if not actual or not actual <= cpus:
            raise AssertionError("thread escaped startup affinity: %r" % thread)
        if actual != timekeeper and actual != services:
            raise AssertionError("unexpected thread affinity: %r" % thread)
    main = next(thread for thread in threads if thread["tid"] == pid)
    if set(main["cpus"]) != services:
        raise AssertionError("main thread is not on service CPUs: %r" % main)

    # Builds with __RESOURCE_QUEUE__ give these roles explicit Linux names.
    named = any(t["name"] in ("TimeKeeper", "ThreadMain") for t in threads)
    if named:
        for name, expected in (("TimeKeeper", timekeeper), ("ThreadMain", services)):
            role = [t for t in threads if t["name"] == name]
            if not role or any(set(t["cpus"]) != expected for t in role):
                raise AssertionError("incorrect %s affinity: %r" % (name, role))
    else:
        # Without names, check both role masks and the main-thread service mask.
        for expected in (timekeeper, services):
            if not any(t["tid"] != pid and set(t["cpus"]) == expected for t in threads):
                raise AssertionError("no child thread has expected mask %r" % expected)


def check_standby_masks(pid, cpus, threads):
    _, services = expected_masks(cpus)
    main = next(thread for thread in threads if thread["tid"] == pid)
    # This path precedes the main thread's bind and ordinary worker startup.
    if set(main["cpus"]) != cpus:
        raise AssertionError("early standby main thread affinity changed: %r" % main)
    workers = [t for t in threads if t["tid"] != pid]
    if len(workers) != 1 or set(workers[0]["cpus"]) != services:
        raise AssertionError("incorrect early standby service affinity: %r" % workers)


def stop_child(process):
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def quote_config(value):
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def run_case(args, name, cpus, output, standby=False):
    case = output / name
    case.mkdir()
    result = {"case": name, "requested_cpus": sorted(cpus), "gtm": str(args.gtm)}
    process = None
    # A short path also keeps the GTM Unix-domain socket below sun_path's limit.
    with tempfile.TemporaryDirectory(prefix="otb-affinity-", dir="/tmp") as temporary, \
            socket.socket() as primary:
        data = Path(temporary)
        (data / "gtm_xlog").mkdir(mode=0o700)
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        # initgtm's XLOG path writes config and creates gtm_xlog; GTM initializes
        # its missing control/store files itself. Do not pre-create gtm.control.
        settings = [
            "nodename = 'affinity_test'", "listen_addresses = '127.0.0.1'",
            "port = %d" % port, "startup = " + ("STANDBY" if standby else "ACT"),
            "unix_socket_directory = " + quote_config(data),
            "log_min_messages = WARNING",
        ]
        if standby:
            # Keep this socket bound but not listening: connection is refused
            # and another process cannot take the selected primary endpoint.
            primary.bind(("127.0.0.1", 0))
            settings += ["active_host = '127.0.0.1'",
                         "active_port = %d" % primary.getsockname()[1]]
        config = "\n".join(settings) + "\n"
        (data / "gtm.conf").write_text(config)
        (case / "gtm.conf").write_text(config)
        try:
            with (case / "stdout.log").open("w") as server_log, \
                    (case / "probe.log").open("w") as probe_log:
                # The parent is single-threaded. Restrict only this child before
                # exec, exactly as taskset does, without changing global state.
                process = subprocess.Popen(
                    [str(args.gtm), "-D", str(data)], stdout=server_log,
                    stderr=subprocess.STDOUT, start_new_session=True,
                    preexec_fn=lambda: os.sched_setaffinity(0, cpus))
                result["pid"] = process.pid
                deadline = time.monotonic() + args.timeout
                while True:
                    if process.poll() is not None:
                        raise AssertionError("GTM exited before readiness: %d" % process.returncode)
                    if time.monotonic() >= deadline:
                        raise AssertionError("GTM client probe did not succeed before timeout")
                    probe_command = [str(args.probe), str(port)]
                    if standby:
                        probe_command.append("status")
                    try:
                        probe = subprocess.run(
                            probe_command, stdout=probe_log,
                            stderr=subprocess.STDOUT, timeout=min(15, max(1, deadline - time.monotonic())))
                    except subprocess.TimeoutExpired:
                        probe_log.write("GTM client probe timed out\n")
                    else:
                        if probe.returncode == 0:
                            break
                    probe_log.flush()
                    time.sleep(0.1)
                if process.poll() is not None:
                    raise AssertionError("GTM exited after the client probe")
                result["threads"] = thread_masks(process.pid)
                if standby:
                    check_standby_masks(process.pid, cpus, result["threads"])
                else:
                    check_masks(process.pid, cpus, result["threads"])
                result["status"] = "PASS"
        except (AssertionError, OSError, subprocess.SubprocessError) as error:
            result["status"] = "FAIL"
            result["error"] = str(error)
            if "threads" not in result and process is not None and process.poll() is None:
                try:
                    result["threads"] = thread_masks(process.pid)
                except FileNotFoundError:
                    pass  # The child may exit between poll() and the /proc read.
        finally:
            if process is not None:
                stop_child(process)
                result["exit_after_cleanup"] = process.returncode
            # Retain logs, not the large freshly initialized GTM store/WAL.
            if (data / "gtm_log").exists():
                shutil.copytree(data / "gtm_log", case / "gtm_log")
            (case / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--gtm", type=Path, required=True, help="built GTM executable (baseline or fixed)")
    parser.add_argument("--probe", type=Path, required=True, help="built affinity_probe executable")
    parser.add_argument("--output-dir", type=Path, help="new directory for retained logs and results")
    parser.add_argument("--timeout", type=float, default=60, help="startup/probe deadline per case, seconds")
    args = parser.parse_args()
    if not sys.platform.startswith("linux") or not hasattr(os, "sched_getaffinity"):
        parser.error("this test requires Linux sched_getaffinity and /proc")
    if args.timeout < 15:
        parser.error("--timeout must be at least 15 seconds (the timestamp probe spans a sync cycle)")
    for name in ("gtm", "probe"):
        path = getattr(args, name).resolve()
        if not path.is_file() or not os.access(path, os.X_OK):
            parser.error("--%s is not executable: %s" % (name, path))
        setattr(args, name, path)
    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    output = (args.output_dir or Path("tmp_check_affinity_%s_%d" % (stamp, os.getpid()))).resolve()
    output.mkdir(parents=True, exist_ok=False)
    available = sorted(os.sched_getaffinity(0))
    print("Available CPUs: %s; logs: %s" % (available, output), flush=True)

    # Choose nonzero CPUs where possible, so hard-coded CPU 0/1 is detected.
    selected = [cpu for cpu in available if cpu != 0] or available
    cases = [("one_cpu", {selected[-1]})]
    skips = []
    if len(available) >= 2:
        cases.append(("two_cpus", set((selected if len(selected) >= 2 else available)[-2:])))
    else:
        skips.append("two_cpus: fewer than two CPUs allowed")
    if len(available) >= 3:
        cases.append(("three_cpus", set(available[:3])))
    else:
        skips.append("three_cpus: fewer than three CPUs allowed")
    if len(selected) >= 2 and selected[-1] - selected[0] > 1:
        cases.append(("sparse_nonzero", {selected[0], selected[-1]}))
    else:
        skips.append("sparse_nonzero: no suitable nonzero noncontiguous mask")
    cases.append(("all_cpus", set(available)))
    cases.append(("standby_unreachable_primary", set(available)))
    results = []
    try:
        for name, cpus in cases:
            result = run_case(args, name, cpus, output, standby=name == "standby_unreachable_primary")
            results.append(result)
            print("%s %s %s%s" % (result["status"], name, sorted(cpus),
                                    ": " + result["error"] if "error" in result else ""), flush=True)
    finally:
        (output / "results.json").write_text(json.dumps(
            {"available_cpus": available, "skipped": skips, "results": results}, indent=2) + "\n")
    for skip in skips:
        print("SKIP " + skip)
    return 1 if any(r["status"] != "PASS" for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
