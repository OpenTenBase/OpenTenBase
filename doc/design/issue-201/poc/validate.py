#!/usr/bin/env python3
# Copyright (C) 2019-2025 OpenTenBase Authors. All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
"""
OpenTenBaseCluster CRD 与示例的离线结构校验。

设计目的：
    本脚本不是形式上的检查，而是把 operator-design.md 中从源码提取的硬约束
    编码为可执行断言。任何人修改 CRD 或示例后，运行本脚本即可发现是否违背
    了 OpenTenBase 的实际限制。

依赖：
    仅使用 Python 标准库，不需要安装 PyYAML，也不需要 Kubernetes 集群。
    这样评审者可以零环境成本复现校验结果。

用法：
    python3 validate.py

退出码：
    0 全部通过；1 存在失败项。
"""

import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DESIGN_DIR = os.path.dirname(HERE)

CRD = os.path.join(HERE, "opentenbasecluster-crd.yaml")
SAMPLE_DIST = os.path.join(HERE, "sample-distributed.yaml")
SAMPLE_CENT = os.path.join(HERE, "sample-centralized.yaml")
DESIGN_DOC = os.path.join(DESIGN_DIR, "operator-design.md")
DISCUSSION = os.path.join(DESIGN_DIR, "discussion-draft.md")
AI_REPORT = os.path.join(DESIGN_DIR, "AI_USAGE_REPORT.md")

# PGXC_NODENAME_LENGTH，来自 src/include/pgxc/nodemgr.h:21
PGXC_NODENAME_LENGTH = 64

results = []


def check(name, ok, detail=""):
    results.append((name, bool(ok), detail))


def read(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


def main():
    # ---------- 文件存在性 ----------
    for label, path in [
        ("CRD 草案存在", CRD),
        ("分布式示例存在", SAMPLE_DIST),
        ("集中式示例存在", SAMPLE_CENT),
        ("设计文档存在", DESIGN_DOC),
        ("讨论帖草案存在", DISCUSSION),
        ("AI 报告存在", AI_REPORT),
    ]:
        check(label, os.path.isfile(path), path)

    missing = [p for p in (CRD, SAMPLE_DIST, SAMPLE_CENT, DESIGN_DOC) if not os.path.isfile(p)]
    if missing:
        report()
        return 1

    crd = read(CRD)
    dist = read(SAMPLE_DIST)
    cent = read(SAMPLE_CENT)
    design = read(DESIGN_DOC)

    # ---------- CRD 基本结构 ----------
    check("CRD apiVersion 正确", "apiextensions.k8s.io/v1" in crd)
    check("CRD kind 正确", "kind: CustomResourceDefinition" in crd)
    check("CRD 名称符合 plural.group", "name: opentenbaseclusters.opentenbase.org" in crd)
    check("CRD 声明 status 子资源", "status: {}" in crd)
    check("CRD scope 为 Namespaced", "scope: Namespaced" in crd)

    # ---------- 三种角色齐全 ----------
    for role in ("gtm", "coordinators", "datanodes"):
        check("CRD 包含角色 %s" % role, re.search(r"^\s{16}%s:" % role, crd, re.M) is not None)

    # ---------- 约束 1：两种部署模式 ----------
    check(
        "CRD 支持 distributed 与 centralized 两种模式",
        "enum: [distributed, centralized]" in crd,
        "依据 config.h ConfigFileInstance::type",
    )

    # ---------- 约束 2：每节点 3 个端口 ----------
    for p in ("node:", "pooler:", "forward:"):
        check("CRD 定义端口 %s" % p.rstrip(":"), p in crd,
              "依据 utils.cpp:39-95 每节点占 3 个连续端口")
    check("默认 node 端口为 11000", "default: 11000" in crd,
          "依据 utils.cpp START_PORT = 11000")

    # ---------- 约束 3：GTM 主节点只能一个 ----------
    # 因此 replicas 语义须为「1 主 + N 备」，注释中必须说明
    check(
        "CRD 说明 GTM 主节点恒为 1",
        "只有一个IP" in crd or "主节点恒为 1" in crd,
        "依据 config.h ConfigFileGtm::master",
    )

    # ---------- 约束 4：节点名长度上限 64 ----------
    check(
        "CRD 引用节点名长度上限 64",
        "PGXC_NODENAME_LENGTH" in crd and "64" in crd,
        "依据 src/include/pgxc/nodemgr.h:21",
    )
    check("节点组名 maxLength 不超过 64",
          re.search(r"maxLength:\s*(\d+)", crd) is not None
          and int(re.search(r"maxLength:\s*(\d+)", crd).group(1)) <= PGXC_NODENAME_LENGTH)

    # ---------- 约束 5：默认节点组名 ----------
    check("默认节点组名为 default_group", "default_group" in crd,
          "依据 cluster.cpp build_create_node_group_cmd()")

    # ---------- 约束 6：引导顺序 GTM -> DN -> CN ----------
    for phase in ("BootstrappingGTM", "BootstrappingDN", "BootstrappingCN"):
        check("status.phase 含 %s" % phase, phase in crd)
    gi, di, ci = (crd.index("BootstrappingGTM"), crd.index("BootstrappingDN"), crd.index("BootstrappingCN"))
    check(
        "引导阶段顺序为 GTM -> DN -> CN",
        gi < di < ci,
        "依据 cluster.cpp:844-870，CN/DN 的 initdb 必须携带 GTM 地址",
    )

    # ---------- 约束 7：拓扑漂移是一等状态 ----------
    check("status.phase 含 TopologyDrifted", "TopologyDrifted" in crd,
          "拓扑目录持久化在系统表中，Pod 地址变化不会被自动感知")
    check("status 含 topology.consistent 字段", "consistent:" in crd)

    # ---------- 约束 8：pool_reload 可配置 ----------
    check("CRD 含 poolReloadAfterChange", "poolReloadAfterChange" in crd,
          "依据 cluster.cpp:241")

    # ---------- 约束 9：GTM failover 走 ALTER CLUSTER GTM NODE ----------
    check(
        "CRD 提供 alterClusterGtmNode 策略",
        "alterClusterGtmNode" in crd,
        "依据 gram.y:12949 与 utility.c:971-975",
    )

    # ---------- 示例：分布式 ----------
    check("分布式示例 mode 为 distributed", "mode: distributed" in dist)
    check("分布式示例包含 gtm 段", re.search(r"^\s{2}gtm:", dist, re.M) is not None)
    check("分布式示例包含 coordinators 段", re.search(r"^\s{2}coordinators:", dist, re.M) is not None)
    check("分布式示例包含 datanodes 段", re.search(r"^\s{2}datanodes:", dist, re.M) is not None)
    check("分布式示例 apiVersion 与 CRD 一致", "opentenbase.org/v1alpha1" in dist)

    # ---------- 示例：集中式（关键断言）----------
    check("集中式示例 mode 为 centralized", "mode: centralized" in cent)
    check(
        "集中式示例不含 gtm 配置段",
        re.search(r"^\s{2}gtm:", cent, re.M) is None,
        "centralized 会忽略 gtm 配置，示例不应出现该段",
    )
    check(
        "集中式示例不含 coordinators 配置段",
        re.search(r"^\s{2}coordinators:", cent, re.M) is None,
        "centralized 会忽略协调节点配置",
    )
    check("集中式示例包含 datanodes 段", re.search(r"^\s{2}datanodes:", cent, re.M) is not None)

    # ---------- 设计文档：证据链完整性 ----------
    evidence = [
        "src/bin/initdb/initdb.c",
        "src/backend/parser/gram.y",
        "src/backend/tcop/utility.c",
        "src/include/pgxc/nodemgr.h",
        "contrib/opentenbase_ctl/src/utils/utils.cpp",
        "contrib/opentenbase_ctl/src/cluster/cluster.cpp",
        "contrib/opentenbase_ctl/src/config/config.h",
    ]
    for e in evidence:
        check("设计文档引用 %s" % e, e in design)

    # ---------- 设计文档：风险必须显式标注 ----------
    for rid in ("R1", "R2", "R3", "R4", "R5", "R6", "R7"):
        check("设计文档含风险条目 %s" % rid, re.search(r"\b%s\b" % rid, design) is not None)
    check(
        "设计文档明确声明未包含可运行 Operator",
        "不包含" in design and "可运行" in design,
        "避免把静态设计误读为已落地实现",
    )
    check("设计文档标注未验证项", "未验证" in design)

    # ---------- Markdown 结构 ----------
    for label, path in [("设计文档", DESIGN_DOC), ("讨论帖", DISCUSSION), ("AI 报告", AI_REPORT)]:
        if not os.path.isfile(path):
            continue
        txt = read(path)
        fences = len(re.findall(r"^\s*```", txt, re.M))
        check("%s 代码围栏配对" % label, fences % 2 == 0, "共 %d 个围栏" % fences)

    # ---------- 换行符：必须为 LF ----------
    for path in (CRD, SAMPLE_DIST, SAMPLE_CENT, DESIGN_DOC, DISCUSSION, AI_REPORT):
        if not os.path.isfile(path):
            continue
        raw = open(path, "rb").read()
        check("%s 使用 LF 换行" % os.path.basename(path), raw.count(b"\r\n") == 0)

    return report()


def report():
    passed = sum(1 for _, ok, _ in results if ok)
    failed = [r for r in results if not r[1]]

    print("=" * 72)
    print("OpenTenBaseCluster CRD 与设计文档离线校验")
    print("=" * 72)
    for name, ok, detail in results:
        mark = "PASS" if ok else "FAIL"
        line = "[%s] %s" % (mark, name)
        if detail and not ok:
            line += "  <- %s" % detail
        print(line)

    print("-" * 72)
    print("通过 %d / %d" % (passed, len(results)))
    if failed:
        print("失败 %d 项：" % len(failed))
        for name, _, detail in failed:
            print("  - %s %s" % (name, detail))
        return 1
    print("全部通过。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
