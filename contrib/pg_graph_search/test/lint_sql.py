#!/usr/bin/env python3
"""SQL 模板静态门禁。

背景（本扩展开发中真实踩过的坑）：
  1. format() 模板串里，RAISE 消息中的字面 % 必须写成 %%，
     否则 format() 把它当占位符 → "unrecognized format() type specifier"。
     只有"位于 format($pgq$ ... $pgq$) / format($pgs$ ... $pgs$) 模板内部"的
     RAISE 才需要转义；普通函数体里的 RAISE 用单 % 是对的。
  2. 递归 CTE 的 anchor 项若带 ORDER BY/LIMIT 必须用子查询包裹，
     否则直接 UNION ALL 会报 "syntax error at or near UNION"。
  3. 递归项子查询的投影若没有 AS 别名，而外层 SELECT 引用 t.<col>，
     运行时报 "column t.xxx does not exist"。
  4. 嵌套 dollar-quote 层级错位（生成代码里 tag 提前闭合）。

用法: python3 test/lint_sql.py
"""
import re
import sys
from pathlib import Path

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"

TEMPLATE_OPEN = ("$pgq$", "$pgs$")
IN_FORMAT_TAGS = ("$pgq$", "$pgs$")


def in_template(lines):
    """标记每一行是否处在 format() 模板串内部。"""
    depth = None          # 当前打开的模板 tag
    inside = []
    for line in lines:
        # 统计该行出现的模板 tag（按出现顺序）
        if depth is None:
            toks = [(m.start(), m.group()) for m in re.finditer(r"\$(?:pgq|pgs)\$", line)]
            if len(toks) >= 1:
                # 同开同闭在同一行 = 完整模板，不算内部
                inside.append(len(toks) == 1 and line.count("format(") > 0 and False)
                # 简化：只有当 tag 数>=2 且成对出现在同一行时不进入
                if len(toks) == 1:
                    inside.append(False)
                    continue
                # 出现 >=2：可能是开+闭，也可能开+关+再开
                # 只有最后一个 tag 未闭合才进入内部
                last = toks[-1][1]
                rest = line[toks[-1][0] + len(last):]
                has_close = bool(re.search(r"\$(?:pgq|pgs)\$", rest))
                if not has_close:
                    depth = last
                inside.append(True)
                continue
            inside.append(False)
        else:
            close = depth
            after = line.find(close)
            if after == -1:
                inside.append(True)
            else:
                inside.append(True)
                depth = None
    return inside


def check(path: Path):
    problems = []
    lines = path.read_text().split("\n")
    flags = in_template(lines)

    for i, (line, ins) in enumerate(zip(lines, flags), 1):
        # 门禁1: format 模板内的 RAISE 字面 % 必须是 %%
        if ins and "RAISE EXCEPTION" in line:
            for m in re.finditer(r"RAISE EXCEPTION '([^']*)'", line):
                msg = m.group(1)
                stripped = msg.replace("%%", "")
                if re.search(r"%(?![\d]+\$s)", stripped):
                    problems.append(
                        (i, f"format 模板内 RAISE 字面 % 未转义: {line.strip()[:90]}"))

        # 门禁3: 递归项投影缺 AS 别名
        if re.search(r"sg\.depth \+ 1(?!\s+AS\b)", line):
            problems.append((i, "递归项 sg.depth+1 缺 AS depth 别名"))
        if re.search(r"sg\.path \|\| e\.(src|dst)(?!\s+AS\b)", line):
            problems.append((i, "递归项 path 投影缺 AS path 别名"))

        # 门禁2: anchor 项带 ORDER BY 但未用子查询包裹
        if re.match(r"\s*SELECT e\.src, e\.dst, e\.prop, e\.w, 1\b", line) \
           and not re.search(r"AS depth\b", line):
            problems.append((i, "anchor 项疑似未包裹（带 ORDER BY/LIMIT 的 anchor 必须包子查询）"))

    # 门禁4: $pga$ 是每个函数体的定界符，必须严格配对。
    #   $pgq$/$pgs$/$pgg$ 出现在生成代码字符串里是合法的单侧使用，不校验奇偶。
    body = "\n".join(l for l in path.read_text().split("\n")
                      if not l.lstrip().startswith(("*", "/*", "*/", "--")))
    for tag in ("$pga$",):
        n = body.count(tag)
        if n % 2 != 0:
            problems.append((0, f"dollar-quote {tag} 出现 {n} 次(奇数)，嵌套层级可能错位"))
    return problems


def main():
    bad = 0
    for f in sorted(SQL_DIR.glob("*.sql")):
        probs = check(f)
        print(f"== {f.name}")
        if not probs:
            print("   OK")
        for ln, msg in probs:
            print(f"   [FAIL] {'行'+str(ln)+': ' if ln else ''}{msg}")
            bad += 1
    print("门禁未通过" if bad else "门禁全部通过")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
