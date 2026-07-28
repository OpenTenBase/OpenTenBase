#!/usr/bin/env python3

import configparser
import re
import subprocess
import sys
from pathlib import Path
from typing import List


ROOT = Path(__file__).resolve().parents[3]
README = ROOT / "README_ZH.md"
CTL_TYPES = ROOT / "contrib/opentenbase_ctl/src/types/types.cpp"
CTL_CONFIG = ROOT / "contrib/opentenbase_ctl/src/config/config.cpp"


def fail(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    raise SystemExit(1)


def fenced_blocks(markdown: str, language: str) -> List[str]:
    pattern = rf"```{language}\n(.*?)\n```"
    return re.findall(pattern, markdown, flags=re.DOTALL)


def check_shell_blocks(markdown: str) -> int:
    blocks = fenced_blocks(markdown, "bash") + fenced_blocks(markdown, "shell")
    for index, block in enumerate(blocks, start=1):
        result = subprocess.run(
            ["bash", "-n"],
            input=block,
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            fail(f"shell block {index} is invalid: {result.stderr.strip()}")
    return len(blocks)


def parse_ini_blocks(markdown: str) -> List[configparser.ConfigParser]:
    parsed = []
    for index, block in enumerate(fenced_blocks(markdown, "ini"), start=1):
        parser = configparser.ConfigParser(interpolation=None)
        try:
            parser.read_string(block)
        except configparser.Error as exc:
            fail(f"INI block {index} is invalid: {exc}")
        parsed.append(parser)
    return parsed


def check_minimum_topology(parsers: List[configparser.ConfigParser]) -> None:
    minimum = next(
        (
            parser
            for parser in parsers
            if parser.get("instance", "name", fallback="")
            == "opentenbase_quickstart"
        ),
        None,
    )
    if minimum is None:
        fail("minimum opentenbase_quickstart INI example is missing")

    expected = {
        ("instance", "type"): "distributed",
        ("gtm", "master"): "127.0.0.1",
        ("coordinators", "master"): "127.0.0.1",
        ("coordinators", "nodes-per-server"): "1",
        ("datanodes", "master"): "127.0.0.1",
        ("datanodes", "nodes-per-server"): "1",
        ("server", "ssh-user"): "opentenbase",
        ("server", "ssh-port"): "22",
    }
    for (section, option), value in expected.items():
        actual = minimum.get(section, option, fallback="")
        if actual != value:
            fail(
                f"minimum INI requires {section}.{option}={value}, got {actual!r}"
            )

    for section in ("coordinators", "datanodes"):
        conf_path = minimum.get(section, "conf", fallback="")
        if not conf_path.startswith("/"):
            fail(f"minimum INI requires an absolute {section}.conf path")

    package = minimum.get("instance", "package", fallback="")
    if not package.endswith(
        "opentenbase-5.21.8-i.REPLACE_WITH_UNAME_M.tar.gz"
    ):
        fail("minimum INI package must use the explicit uname -m placeholder")


def check_ini_package_architecture(
    parsers: List[configparser.ConfigParser],
) -> None:
    expected_suffix = "opentenbase-5.21.8-i.REPLACE_WITH_UNAME_M.tar.gz"
    for index, parser in enumerate(parsers, start=1):
        package = parser.get("instance", "package", fallback="")
        if package and not package.endswith(expected_suffix):
            fail(f"INI block {index} package must use the uname -m placeholder")


def check_architecture_contract(markdown: str) -> None:
    required_snippets = (
        'PACKAGE_ARCH=$(uname -m)',
        'opentenbase-5.21.8-i.${PACKAGE_ARCH}.tar.gz',
        "opentenbase-5.21.8-i.REPLACE_WITH_UNAME_M.tar.gz",
        "aarch64-unknown-linux-gnu",
        "ARM64_MINIMAL_DEPLOYMENT=PASS",
    )
    for snippet in required_snippets:
        if snippet not in markdown:
            fail(f"missing architecture-neutral quick-start text: {snippet}")


def check_troubleshooting(markdown: str) -> None:
    required_headings = (
        "### `psql` 或 `opentenbase_ctl` 命令未找到",
        "### 程序提示 `error while loading shared libraries`",
        "### `opentenbase_ctl` 无输出或长时间等待",
        "### `Failed to parse configuration file`",
        "### SSH 认证失败、连接超时或端口被拒绝",
        "### 节点显示 `Unknown`，或客户端连接被拒绝",
    )
    for heading in required_headings:
        if heading not in markdown:
            fail(f"missing troubleshooting heading: {heading}")


def check_relative_links(markdown: str) -> int:
    checked = 0
    for target in re.findall(r"\[[^\]]+\]\(([^)]+)\)", markdown):
        if re.match(r"^(?:https?://|mailto:|#)", target):
            continue
        relative = target.split("#", 1)[0]
        if not relative:
            continue
        checked += 1
        if not (ROOT / relative).exists():
            fail(f"relative link does not exist: {target}")
    return checked


def check_password_redaction() -> None:
    types_source = CTL_TYPES.read_text(encoding="utf-8")
    plaintext_log = re.compile(
        r"LOG_[A-Z_]+_FMT\s*\([^;]*server\.ssh_password\.c_str\s*\(\s*\)",
        flags=re.DOTALL,
    )
    if plaintext_log.search(types_source):
        fail("opentenbase_ctl must not write the SSH password in config summaries")
    if '"********"' not in types_source:
        fail("opentenbase_ctl config summary must use an explicit password mask")

    config_source = CTL_CONFIG.read_text(encoding="utf-8")
    plaintext_parser_log = re.compile(
        r'LOG_DEBUG_FMT\s*\(\s*"Parsing config item: %s = %s"\s*,'
        r"\s*key\.c_str\s*\(\s*\)\s*,\s*value\.c_str\s*\(\s*\)",
        flags=re.DOTALL,
    )
    if plaintext_parser_log.search(config_source):
        fail("opentenbase_ctl must not write the SSH password while parsing config")
    section_limited_mask = re.compile(
        r'section\s*==\s*"server"\s*&&\s*key\s*==\s*"ssh-password"'
    )
    if section_limited_mask.search(config_source):
        fail("opentenbase_ctl must mask misplaced SSH password config items")
    if not re.search(r'log_value\s*=\s*key\s*==\s*"ssh-password"', config_source):
        fail("opentenbase_ctl config parser must mask the SSH password by key")
    if '"********"' not in config_source:
        fail("opentenbase_ctl config parser must use an explicit password mask")


def main() -> None:
    markdown = README.read_text(encoding="utf-8")
    shell_count = check_shell_blocks(markdown)
    ini_parsers = parse_ini_blocks(markdown)
    if len(ini_parsers) < 3:
        fail(f"expected at least 3 INI examples, found {len(ini_parsers)}")
    check_minimum_topology(ini_parsers)
    check_ini_package_architecture(ini_parsers)
    check_architecture_contract(markdown)
    check_troubleshooting(markdown)
    link_count = check_relative_links(markdown)
    check_password_redaction()
    print(
        "PASS: "
        f"{shell_count} shell blocks, {len(ini_parsers)} INI examples, "
        f"{link_count} relative links, ARM64 quick-start contract, "
        "and credential redaction"
    )


if __name__ == "__main__":
    main()
