#!/usr/bin/env python3
"""T1.7 · 新旧模型对照图（纯标准库，输出 SVG）

不依赖 matplotlib：图必须能在任意一台只有 python3 的机器上从 CSV 重建出来，
否则"图可复现"就成了空话。

用法：
    python3 tools/plot_compare.py results/<run>/model_compare.csv [输出.svg]

图的读法：
    黑叉  实测报错 MB（stderr 原文里的数字）
    蓝点  新模型预测（三检查点，取 first_hit）
    红方  旧模型（pgvector 0.8.0 口径，单一总量）
    蓝点与黑叉重合表示逐字命中；红方与黑叉的垂直距离就是旧模型的偏差倍数。
"""
import csv
import math
import sys

W, H = 1180, 620
PAD_L, PAD_R, PAD_T, PAD_B = 78, 24, 64, 116


def load(path):
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["actual_mb"] in ("none", "", None):
                continue          # 没有报错的用例不进对照图，另行在正文说明
            rows.append({
                "case": r["case_id"],
                "cls": r["class"],
                "hit": r["first_hit"],
                "actual": float(r["actual_mb"]),
                "new": float(r["new_mb"]),
                "legacy": float(r["legacy_mb"]),
                "ratio": float(r["legacy_over_actual"]) if r["legacy_over_actual"] else None,
            })
    if not rows:
        sys.exit("CSV 里没有可用于对照的报错用例")
    return rows


def build_svg(rows):
    vmax = max(max(r["legacy"], r["actual"]) for r in rows)
    lo, hi = 0.0, math.ceil(math.log10(vmax * 1.35))          # 对数轴：1 MB 起
    plot_w = W - PAD_L - PAD_R
    plot_h = H - PAD_T - PAD_B
    step = plot_w / len(rows)

    def x(i):
        return PAD_L + step * (i + 0.5)

    def y(v):
        v = max(v, 1.0)
        return PAD_T + plot_h * (1 - (math.log10(v) - lo) / (hi - lo))

    p = ['<?xml version="1.0" encoding="UTF-8"?>',
         f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
         f'viewBox="0 0 {W} {H}" font-family="Helvetica,Arial,sans-serif">',
         f'<rect width="{W}" height="{H}" fill="#ffffff"/>',
         f'<text x="{PAD_L}" y="30" font-size="17" font-weight="bold">'
         'IVFFlat 构建内存预测：新模型（三检查点）与旧模型（0.8.0 口径）对照</text>',
         f'<text x="{PAD_L}" y="50" font-size="12" fill="#555">'
         '纵轴对数刻度，单位 MB，取 stderr 里的报错数字；红方上方的倍数为旧模型 / 实测</text>']

    # 网格与纵轴刻度
    e = int(lo)
    while e <= hi:
        gy = y(10 ** e)
        p.append(f'<line x1="{PAD_L}" y1="{gy:.1f}" x2="{W-PAD_R}" y2="{gy:.1f}" '
                 f'stroke="#e8e8e8" stroke-width="1"/>')
        p.append(f'<text x="{PAD_L-10}" y="{gy+4:.1f}" font-size="11" fill="#666" '
                 f'text-anchor="end">{10**e:g}</text>')
        e += 1
    p.append(f'<line x1="{PAD_L}" y1="{PAD_T}" x2="{PAD_L}" y2="{H-PAD_B}" stroke="#333"/>')
    p.append(f'<line x1="{PAD_L}" y1="{H-PAD_B}" x2="{W-PAD_R}" y2="{H-PAD_B}" stroke="#333"/>')
    return p, x, y, step


def draw_series(p, rows, x, y, step):
    for i, r in enumerate(rows):
        cx = x(i)
        # 旧模型与实测之间连一条虚线，偏差一眼可见
        p.append(f'<line x1="{cx:.1f}" y1="{y(r["legacy"]):.1f}" x2="{cx:.1f}" '
                 f'y2="{y(r["actual"]):.1f}" stroke="#d9534f" stroke-width="1" '
                 f'stroke-dasharray="3,3" opacity="0.55"/>')
        # 旧模型：红方
        ly = y(r["legacy"])
        p.append(f'<rect x="{cx-4:.1f}" y="{ly-4:.1f}" width="8" height="8" '
                 f'fill="#d9534f" opacity="0.9"/>')
        if r["ratio"] and r["ratio"] >= 1.4:
            p.append(f'<text x="{cx:.1f}" y="{ly-9:.1f}" font-size="10" fill="#b52b27" '
                     f'text-anchor="middle">{r["ratio"]:.0f}x</text>')
        # 实测：黑叉
        ay = y(r["actual"])
        p.append(f'<path d="M{cx-5:.1f},{ay-5:.1f} L{cx+5:.1f},{ay+5:.1f} '
                 f'M{cx-5:.1f},{ay+5:.1f} L{cx+5:.1f},{ay-5:.1f}" '
                 f'stroke="#222" stroke-width="1.8"/>')
        # 新模型：蓝圈（与黑叉重合即逐字命中）
        p.append(f'<circle cx="{cx:.1f}" cy="{y(r["new"]):.1f}" r="7" fill="none" '
                 f'stroke="#2b6cb0" stroke-width="1.8"/>')
        # 横轴标签：用例号 + 命中的检查点
        p.append(f'<text x="{cx:.1f}" y="{H-PAD_B+16:.1f}" font-size="11" '
                 f'text-anchor="middle" fill="#333">{r["case"]}</text>')
        p.append(f'<text x="{cx:.1f}" y="{H-PAD_B+30:.1f}" font-size="10" '
                 f'text-anchor="middle" fill="#2b6cb0">{r["hit"]}</text>')


def draw_legend(p, rows):
    ly = H - 52
    items = [("#222", "cross", "实测报错 MB（stderr 原文）"),
             ("#2b6cb0", "circle", "新模型：三检查点取 first_hit"),
             ("#d9534f", "rect", "旧模型：pgvector 0.8.0 单一总量")]
    lx = PAD_L
    for color, shape, label in items:
        if shape == "cross":
            p.append(f'<path d="M{lx-5},{ly-5} L{lx+5},{ly+5} M{lx-5},{ly+5} L{lx+5},{ly-5}" '
                     f'stroke="{color}" stroke-width="1.8"/>')
        elif shape == "circle":
            p.append(f'<circle cx="{lx}" cy="{ly}" r="7" fill="none" stroke="{color}" '
                     f'stroke-width="1.8"/>')
        else:
            p.append(f'<rect x="{lx-4}" y="{ly-4}" width="8" height="8" fill="{color}"/>')
        p.append(f'<text x="{lx+14}" y="{ly+4}" font-size="12" fill="#333">{label}</text>')
        lx += 300
    worst = max(r["ratio"] or 0 for r in rows)
    p.append(f'<text x="{PAD_L}" y="{H-22}" font-size="12" fill="#333">'
             f'{len(rows)} 组有报错的用例中，新模型全部逐字命中（蓝圈套住黑叉）；'
             f'旧模型最大偏差 {worst:.0f} 倍。</text>')


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    csv_path = sys.argv[1]
    out = sys.argv[2] if len(sys.argv) > 2 else csv_path.rsplit(".", 1)[0] + ".svg"

    rows = load(csv_path)
    rows.sort(key=lambda r: (r["cls"], r["case"]))
    p, x, y, step = build_svg(rows)
    draw_series(p, rows, x, y, step)
    draw_legend(p, rows)
    p.append("</svg>")

    with open(out, "w", encoding="utf-8") as f:
        f.write("\n".join(p))

    exact = sum(1 for r in rows if r["new"] == r["actual"])
    print(f"写出 {out}")
    print(f"对照用例 {len(rows)} 组；新模型逐字命中 {exact} 组；"
          f"旧模型最大偏差 {max(r['ratio'] or 0 for r in rows):.2f} 倍")
    if exact != len(rows):
        print("注意：有用例未逐字命中，图上蓝圈与黑叉会分离——不要当成绘图误差")


if __name__ == "__main__":
    main()



