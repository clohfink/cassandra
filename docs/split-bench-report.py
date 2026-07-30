#!/usr/bin/env python3
"""
Turn one or more LargeSSTableSplitBench --csv files into a single self-contained HTML comparison.

    python3 docs/split-bench-report.py run1.csv [run2.csv ...] -o docs/split-bench-comparison.html

No dependencies and no network: the charts are inline SVG with a small hover layer, so the output is
one file you can attach to a review. Colours are the validated two-slot categorical palette
(blue = zero copy, orange = the existing rewrite); both modes pass all six checks of the dataviz
validator, so identity is never carried by colour alone -- every series is also directly labelled and
present in the table at the bottom.
"""
import argparse
import csv
import html
import math
import os
from collections import defaultdict

MIB = 1024.0 * 1024.0

# Validated categorical slots 1-4, light / dark. Do not re-pick these by eye: the order below is the one
# the validator was run against (`--pairs adjacent`, both modes). It is not the semantically obvious order --
# leading with the rewrite would put blue next to aqua, whose tritan separation in dark mode is only ΔE 4.0.
# Light mode raises a contrast WARN for aqua and yellow, which the relief rule covers here: every mark is
# directly labelled and the full table is present.
SERIES = {
    "copy":       {"label": "zero-copy, byte copy", "light": "#2a78d6", "dark": "#3987e5",
                   "note": "reflink off, digest on — what shipped before reflink"},
    "rewrite":    {"label": "existing rewrite", "light": "#eb6834", "dark": "#d95926",
                   "note": "SSTableSplitter, the full deserialise/reserialise path"},
    "clone":      {"label": "zero-copy, cloned", "light": "#1baf7a", "dark": "#199e70",
                   "note": "reflink on, digest on — extents shared, digest still read"},
    "clone_nodigest": {"label": "zero-copy, cloned, no digest", "light": "#eda100", "dark": "#c98500",
                   "note": "reflink on, digest off — the floor this tool can measure"},
}
ORDER = ["copy", "rewrite", "clone", "clone_nodigest"]


INT_COLUMNS = ("parent_on_disk", "parent_uncompressed", "partitions", "rows", "chunk_length",
               "children_requested", "iteration", "rchar", "read_bytes", "wchar", "write_bytes",
               "written_bytes", "alloc_bytes", "children_produced", "dead_prefix", "duplicated_chunk",
               "cloned_bytes", "head_pad")


def load(specs):
    """Each spec is CONFIG=path.csv. The config names which (reflink, digest) variant the file holds; the
    baseline rows in any file are the same rewrite regardless, so they all fold into one 'rewrite' series."""
    rows = []
    for spec in specs:
        config, _, path = spec.partition("=")
        if not path:
            config, path = "copy", spec
        if config not in SERIES:
            raise SystemExit(f"unknown config {config!r}; expected one of {', '.join(SERIES)}")
        with open(path, newline="") as handle:
            for row in csv.DictReader(handle):
                if not row.get("path"):
                    continue
                r = dict(row)
                for key in INT_COLUMNS:
                    value = row.get(key)
                    r[key] = int(float(value)) if value not in (None, "") else -1
                r["wall_ms"] = float(row["wall_ms"])
                r["w_amp"] = float(row["w_amp"])
                r["parent_mib"] = r["parent_on_disk"] / MIB
                r["wall_s"] = r["wall_ms"] / 1000.0
                r["mibps"] = (r["parent_mib"] / r["wall_s"]) if r["wall_s"] else 0.0
                r["config"] = "rewrite" if r["path"] == "baseline" else config
                # What the split actually put on the platter. A cloned range shares extents with the parent,
                # so it occupies no new space however long the child file reports itself to be.
                r["physical_mib"] = max(0, r["written_bytes"] - max(0, r["cloned_bytes"])) / MIB
                rows.append(r)
    return rows


def median(values):
    values = sorted(values)
    if not values:
        return 0.0
    mid = len(values) // 2
    return values[mid] if len(values) % 2 else (values[mid - 1] + values[mid]) / 2.0


def collapse(rows, cache):
    """One point per (shape, config): the median across iterations. Keyed so sizes line up."""
    buckets = defaultdict(list)
    for r in rows:
        if r["evict"] != cache:
            continue
        buckets[(r["shape"], r["config"])].append(r)
    points = {}
    for (shape, config), group in buckets.items():
        first = group[0]
        points[(shape, config)] = {
            "shape": shape,
            "path": config,
            "parent_mib": first["parent_mib"],
            "partitions": first["partitions"],
            "children": first["children_requested"],
            "key_type": shape.rsplit("-", 1)[-1],
            "wall_s": median([g["wall_s"] for g in group]),
            "mibps": median([g["mibps"] for g in group]),
            "alloc_mib": median([g["alloc_bytes"] for g in group]) / MIB,
            "rd_mib": median([g["read_bytes"] for g in group]) / MIB,
            "wr_mib": median([g["write_bytes"] for g in group]) / MIB,
            "w_amp": median([g["w_amp"] for g in group]),
            "physical_mib": median([g["physical_mib"] for g in group]),
            "cloned_mib": median([max(0, g["cloned_bytes"]) for g in group]) / MIB,
            "dead": first["dead_prefix"],
            "dup": first["duplicated_chunk"],
            "n": len(group),
        }
    return points


def nice_ticks(lo, hi, count=5):
    if hi <= lo:
        return [lo]
    raw = (hi - lo) / count
    magnitude = 10 ** math.floor(math.log10(raw))
    for step in (1, 2, 2.5, 5, 10):
        if raw <= step * magnitude:
            step *= magnitude
            break
    start = math.floor(lo / step) * step
    ticks, value = [], start
    while value <= hi + step * 0.5:
        if value >= lo - step * 0.001:
            ticks.append(value)
        value += step
    return ticks


def fmt(value):
    if value == 0:
        return "0"
    if value >= 1000:
        return f"{value:,.0f}"
    if value >= 100:
        return f"{value:.0f}"
    if value >= 10:
        return f"{value:.1f}"
    if value >= 1:
        return f"{value:.2f}"
    return f"{value:.3f}"


def line_chart(title, subtitle, series, ylabel, log_y=False, unit=""):
    """A log-x line chart with markers, direct labels at the last point, and a hover layer."""
    W, H = 760, 340
    ML, MR, MT, MB = 74, 132, 16, 46
    pw, ph = W - ML - MR, H - MT - MB

    xs = [p["parent_mib"] for pts in series.values() for p in pts]
    ys = [p["y"] for pts in series.values() for p in pts]
    if not xs or not ys:
        return f'<p class="empty">no data for {html.escape(title)}</p>'

    lx0, lx1 = math.log10(min(xs)), math.log10(max(xs))
    if lx1 - lx0 < 0.3:
        lx0, lx1 = lx0 - 0.25, lx1 + 0.25

    if log_y:
        ly0, ly1 = math.log10(max(min(ys) * 0.6, 1e-3)), math.log10(max(ys) * 1.8)
        ypos = lambda v: MT + ph - (math.log10(max(v, 1e-3)) - ly0) / (ly1 - ly0) * ph
        yt = [10 ** e for e in range(math.floor(ly0), math.ceil(ly1) + 1)]
    else:
        y1 = max(ys) * 1.15
        ypos = lambda v: MT + ph - (v / y1) * ph
        yt = nice_ticks(0, y1, 5)

    xpos = lambda v: ML + (math.log10(v) - lx0) / (lx1 - lx0) * pw

    out = [f'<svg viewBox="0 0 {W} {H}" role="img" class="chart" aria-label="{html.escape(title)}">']
    for t in yt:
        y = ypos(t)
        if not (MT - 1 <= y <= MT + ph + 1):
            continue
        out.append(f'<line class="grid" x1="{ML}" y1="{y:.1f}" x2="{ML + pw}" y2="{y:.1f}"/>')
        out.append(f'<text class="tick" x="{ML - 9}" y="{y + 4:.1f}" text-anchor="end">{fmt(t)}</text>')
    # Draw a gridline per distinct size but only label where there is room: two corpora of nearly the
    # same size sit on top of each other otherwise, which is how "2 GiB 2 GiB" became "22GGiB".
    drawn = -1e9
    for t in sorted(set(xs)):
        x = xpos(t)
        out.append(f'<line class="grid vgrid" x1="{x:.1f}" y1="{MT}" x2="{x:.1f}" y2="{MT + ph}"/>')
        if x - drawn < 46:
            continue
        drawn = x
        label = f"{t / 1024:.1f} GiB" if t >= 1024 else f"{t:.0f} MiB"
        out.append(f'<text class="tick" x="{x:.1f}" y="{MT + ph + 20}" text-anchor="middle">{label}</text>')
    out.append(f'<text class="axis" x="{ML + pw / 2:.0f}" y="{H - 6}" text-anchor="middle">'
               'parent size on disk (log scale)</text>')
    out.append(f'<text class="axis" transform="translate(16,{MT + ph / 2:.0f}) rotate(-90)" '
               f'text-anchor="middle">{html.escape(ylabel)}</text>')

    taken = []
    for name in ORDER:
        pts = sorted(series.get(name, []), key=lambda p: p["parent_mib"])
        if not pts:
            continue
        colour = f"var(--s-{name})"
        d = " ".join(("M" if i == 0 else "L") + f"{xpos(p['parent_mib']):.1f},{ypos(p['y']):.1f}"
                     for i, p in enumerate(pts))
        out.append(f'<path class="line" d="{d}" stroke="{colour}"/>')
        for p in pts:
            x, y = xpos(p["parent_mib"]), ypos(p["y"])
            tip = (f"{SERIES[name]['label']} &mdash; {fmt(p['parent_mib'] / 1024)} GiB parent, "
                   f"{p['children']} children<br>{html.escape(ylabel)}: <b>{fmt(p['y'])}{unit}</b>"
                   f"<br>{p['partitions']:,} partitions &middot; median of {p['n']}")
            out.append(f'<circle class="dot" cx="{x:.1f}" cy="{y:.1f}" r="5.5" fill="{colour}" '
                       f'tabindex="0" data-tip="{html.escape(tip, quote=True)}"/>')
        # Direct-label the last point, nudged clear of any label already placed there. When the series
        # converge -- which is the whole finding at large sizes -- they would otherwise print on top of
        # each other and neither would be readable.
        last = pts[-1]
        ly = ypos(last["y"]) + 4
        while any(abs(ly - t) < 15 for t in taken):
            ly += 15
        taken.append(ly)
        out.append(f'<text class="direct" x="{xpos(last["parent_mib"]) + 12:.1f}" '
                   f'y="{ly:.1f}" fill="{colour}">{html.escape(SERIES[name]["label"])}</text>')

    out.append("</svg>")
    return (f'<figure><figcaption><h3>{html.escape(title)}</h3>'
            f'<p>{subtitle}</p></figcaption>{"".join(out)}</figure>')


def shape_detail(shape):
    """The distinguishing tail of a shape label: value size and key type, which is what actually varies."""
    parts = shape.split("-")
    value = next((x for x in parts if x.startswith("v")), "")
    key = parts[-1] if parts[-1] in ("blob", "uuid") else ""
    value = value[1:] if value else ""
    if value and value.isdigit():
        value = f"{int(value) // 1024} KiB rows" if int(value) >= 1024 else f"{value} B rows"
    return " · ".join(x for x in (value, key) if x)


def config_bars(title, subtitle, points, field, unit, fmt_value=None):
    """One row group per parent size, one bar per configuration. Every bar carries its own value label,
    which is also the relief the light-mode contrast WARN requires."""
    fmt_value = fmt_value or (lambda v: fmt(v) + unit)
    sizes = sorted({(p["parent_mib"], p["shape"]) for p in points.values()})
    if not sizes:
        return ""
    W = 760
    bar_h, group_gap = 22, 16
    ML, MR, MT = 96, 128, 34
    present = [c for c in ORDER if any(k[1] == c for k in points)]
    H = MT + sum(bar_h * len([c for c in present if (sh, c) in points]) + group_gap for _, sh in sizes) + 8
    widest = max(p[field] for p in points.values()) or 1
    scale = (W - ML - MR) / widest

    out = [f'<svg viewBox="0 0 {W} {H}" role="img" class="chart" aria-label="{html.escape(title)}">']
    for i, c in enumerate(present):
        out.append(f'<rect x="{ML + i * 172}" y="8" width="9" height="9" rx="2" fill="var(--s-{c})"/>')
        out.append(f'<text class="tick" x="{ML + i * 172 + 14}" y="17">'
                   f'{html.escape(SERIES[c]["label"])}</text>')
    y = MT
    for size, shape in sizes:
        label = f"{size / 1024:.1f} GiB" if size >= 1024 else f"{size:.0f} MiB"
        rows_here = [c for c in present if (shape, c) in points]
        mid = y + bar_h * len(rows_here) / 2
        out.append(f'<text class="tick strong" x="{ML - 12}" y="{mid - 2:.0f}" '
                   f'text-anchor="end">{label}</text>')
        # Several corpora can be near the same size and differ only in row shape or key type, so name the
        # part of the shape that actually distinguishes them rather than printing "8 GiB" three times.
        detail = shape_detail(shape)
        if detail:
            out.append(f'<text class="tick" x="{ML - 12}" y="{mid + 11:.0f}" text-anchor="end" '
                       f'style="font-size:9.5px">{html.escape(detail)}</text>')
        for c in rows_here:
            p = points[(shape, c)]
            w = max(p[field] * scale, 1.5)
            tip = (f"{SERIES[c]['label']}<br><i>{SERIES[c]['note']}</i><br>{html.escape(title)}: "
                   f"<b>{fmt_value(p[field])}</b><br>{p['partitions']:,} partitions, {p['children']} children"
                   f" &middot; median of {p['n']}")
            out.append(f'<rect class="bar" x="{ML}" y="{y + 3}" width="{w:.1f}" height="{bar_h - 6}" rx="2" '
                       f'fill="var(--s-{c})" tabindex="0" data-tip="{html.escape(tip, quote=True)}"/>')
            out.append(f'<text class="direct small" x="{ML + w + 8:.1f}" y="{y + bar_h - 6}" '
                       f'fill="var(--s-{c})">{html.escape(fmt_value(p[field]))}</text>')
            y += bar_h
        y += group_gap
    out.append("</svg>")
    return (f'<figure><figcaption><h3>{html.escape(title)}</h3><p>{subtitle}</p></figcaption>'
            f'{"".join(out)}</figure>')


def grouped_bars(title, subtitle, points):
    """Device read and write bytes, per path, per size. Two bars per group, 2px surface gap."""
    shapes = sorted({p["shape"] for p in points.values()}, key=lambda s: dict(
        (q["shape"], q["parent_mib"]) for q in points.values())[s])
    if not shapes:
        return ""
    W = 760
    row_h, gap = 30, 20
    ML, MR, MT = 150, 90, 30
    H = MT + len(shapes) * (row_h * 2 + gap) + 20
    widest = max((points[(s, p)]["rd_mib"] + points[(s, p)]["wr_mib"])
                 for s in shapes for p in ORDER if (s, p) in points)
    scale = (W - ML - MR) / widest if widest else 1

    out = [f'<svg viewBox="0 0 {W} {H}" role="img" class="chart" aria-label="{html.escape(title)}">']
    out.append(f'<text class="tick" x="{ML}" y="{MT - 12}">read (solid) + write (hatched), MiB '
               'that crossed the block layer</text>')
    y = MT
    for shape in shapes:
        size = points[(shape, ORDER[0])]["parent_mib"] if (shape, ORDER[0]) in points else 0
        label = f"{size / 1024:.1f} GiB" if size >= 1024 else f"{size:.0f} MiB"
        # Two corpora can be the same size and differ only in shape (jitter, key type), so name the shape
        # too rather than printing "2 GiB" twice with no way to tell them apart.
        detail = shape.split("-", 1)[1] if "-" in shape else shape
        out.append(f'<text class="tick strong" x="{ML - 12}" y="{y + row_h - 8}" '
                   f'text-anchor="end">{label}</text>')
        out.append(f'<text class="tick" x="{ML - 12}" y="{y + row_h + 6}" text-anchor="end" '
                   f'style="font-size:9.5px">{html.escape(detail)}</text>')
        for name in ORDER:
            if (shape, name) not in points:
                y += row_h
                continue
            p = points[(shape, name)]
            colour = f"var(--s-{name})"
            rw, ww = p["rd_mib"] * scale, p["wr_mib"] * scale
            out.append(f'<rect class="bar" x="{ML}" y="{y + 5}" width="{max(rw, 1):.1f}" '
                       f'height="{row_h - 12}" fill="{colour}" rx="2" tabindex="0" '
                       f'data-tip="{html.escape(SERIES[name]["label"])} read: <b>{fmt(p["rd_mib"])} MiB</b>"/>')
            out.append(f'<rect class="bar" x="{ML + rw + 2:.1f}" y="{y + 5}" width="{max(ww, 1):.1f}" '
                       f'height="{row_h - 12}" fill="url(#hatch-{name})" stroke="{colour}" rx="2" '
                       f'tabindex="0" data-tip="{html.escape(SERIES[name]["label"])} write: '
                       f'<b>{fmt(p["wr_mib"])} MiB</b>"/>')
            out.append(f'<text class="direct small" x="{ML + rw + ww + 10:.1f}" y="{y + row_h - 8}" '
                       f'fill="{colour}">{fmt(p["rd_mib"] + p["wr_mib"])}</text>')
            y += row_h
        y += gap
    out.append("</svg>")
    defs = "".join(
        f'<pattern id="hatch-{n}" patternUnits="userSpaceOnUse" width="6" height="6" '
        f'patternTransform="rotate(45)"><rect width="6" height="6" fill="var(--surface-1)"/>'
        f'<line x1="0" y1="0" x2="0" y2="6" stroke="var(--s-{n})" stroke-width="3"/></pattern>'
        for n in ORDER)
    return (f'<figure><figcaption><h3>{html.escape(title)}</h3><p>{subtitle}</p></figcaption>'
            f'<svg width="0" height="0"><defs>{defs}</defs></svg>{"".join(out)}</figure>')


def table(rows):
    head = ("shape", "cache", "configuration", "it", "parent MiB", "partitions", "kids",
            "wall s", "MiB/s", "new bytes MiB", "cloned MiB", "rd disk MiB", "alloc MiB", "w_amp",
            "dead prefix", "head pad")
    out = ['<table><thead><tr>' + "".join(f"<th>{h}</th>" for h in head) + "</tr></thead><tbody>"]
    for r in sorted(rows, key=lambda r: (r["parent_mib"], r["evict"], r["config"], r["iteration"])):
        cloned = max(0, r["cloned_bytes"]) / MIB if r["cloned_bytes"] >= 0 else None
        pad = max(0, r["head_pad"]) / 1024 if r["head_pad"] >= 0 else None
        cells = (r["shape"], r["evict"], SERIES.get(r["config"], {}).get("label", r["config"]),
                 r["iteration"], fmt(r["parent_mib"]), "{:,}".format(r["partitions"]),
                 r["children_produced"], "{:.2f}".format(r["wall_s"]), fmt(r["mibps"]),
                 "{:,.0f}".format(r["physical_mib"]),
                 "-" if cloned is None else "{:,.0f}".format(cloned),
                 fmt(r["read_bytes"] / MIB) if r["read_bytes"] >= 0 else "-",
                 fmt(r["alloc_bytes"] / MIB) if r["alloc_bytes"] >= 0 else "-",
                 "{:.4f}".format(r["w_amp"]),
                 "{:.0f} KiB".format(r["dead_prefix"] / 1024) if r["dead_prefix"] >= 0 else "-",
                 "-" if pad is None else "{:.0f} KiB".format(pad))
        marker = ' class="c-' + r["config"] + '"'
        out.append(f"<tr{marker}>" + "".join(f"<td>{html.escape(str(c))}</td>" for c in cells) + "</tr>")
    out.append("</tbody></table>")
    return "".join(out)


def hero(cold):
    """The numbers that actually decide the question, taken at the largest parent measured."""
    best = "clone_nodigest" if any(k[1] == "clone_nodigest" for k in cold) else "copy"
    shapes = [sh for sh in {k[0] for k in cold} if (sh, best) in cold and (sh, "rewrite") in cold]
    if not shapes:
        return "", None
    shape = max(shapes, key=lambda sh: cold[(sh, best)]["parent_mib"])
    zc, bl = cold[(shape, best)], cold[(shape, "rewrite")]
    speedup = bl["wall_s"] / zc["wall_s"] if zc["wall_s"] else 0
    alloc = bl["alloc_mib"] / zc["alloc_mib"] if zc["alloc_mib"] else 0
    written = (bl["physical_mib"] / zc["physical_mib"]) if zc["physical_mib"] > 0.5 else None
    tiles = [
        ("wall clock", f"{speedup:,.0f}×" if speedup >= 10 else f"{speedup:.2f}×",
         f"{SERIES[best]['label']} vs the rewrite, {zc['parent_mib'] / 1024:.0f} GiB cold"),
        ("bytes written", "∞" if written is None else f"{written:,.0f}×",
         "less put on the platter — a cloned extent occupies no new space"),
        ("allocation", f"{alloc:,.0f}×", "less heap churned for the same split"),
    ]
    cards = "".join(f'<div class="tile"><span class="k">{html.escape(k)}</span>'
                    f'<span class="v">{html.escape(v)}</span>'
                    f'<span class="c">{html.escape(c)}</span></div>' for k, v, c in tiles)
    return f'<section class="tiles">{cards}</section>', (speedup, alloc, zc, bl, best, written)


CSS = """
:root { color-scheme: light dark; }
.viz-root {
  --surface-1: #fcfcfb; --surface-2: #f4f4f2; --border: #dedcd5;
  --text-primary: #0b0b0b; --text-secondary: #52514e; --text-muted: #78766f;
  --s-copy: #2a78d6; --s-rewrite: #eb6834; --s-clone: #1baf7a; --s-clone_nodigest: #eda100;
}
@media (prefers-color-scheme: dark) {
  :root:where(:not([data-theme="light"])) .viz-root {
    --surface-1: #1a1a19; --surface-2: #232322; --border: #3a3a38;
    --text-primary: #ffffff; --text-secondary: #c3c2b7; --text-muted: #9b9a90;
    --s-copy: #3987e5; --s-rewrite: #d95926; --s-clone: #199e70; --s-clone_nodigest: #c98500;
  }
}
:root[data-theme="dark"] .viz-root {
  --surface-1: #1a1a19; --surface-2: #232322; --border: #3a3a38;
  --text-primary: #ffffff; --text-secondary: #c3c2b7; --text-muted: #9b9a90;
  --s-copy: #3987e5; --s-rewrite: #d95926; --s-clone: #199e70; --s-clone_nodigest: #c98500;
}
* { box-sizing: border-box; }
body { margin: 0; background: var(--surface-2); }
.viz-root { background: var(--surface-2); color: var(--text-primary); min-height: 100vh;
  font: 15px/1.55 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; padding: 32px 20px 64px; }
.wrap { max-width: 900px; margin: 0 auto; }
h1 { font-size: 26px; margin: 0 0 6px; letter-spacing: -.01em; }
h2 { font-size: 18px; margin: 40px 0 10px; padding-bottom: 6px; border-bottom: 1px solid var(--border); }
h3 { font-size: 15px; margin: 0 0 2px; }
p { color: var(--text-secondary); margin: 6px 0; }
.lede { font-size: 16px; }
.tiles { display: flex; gap: 12px; flex-wrap: wrap; margin: 22px 0 8px; }
.tile { flex: 1 1 180px; background: var(--surface-1); border: 1px solid var(--border);
  border-radius: 10px; padding: 14px 16px; }
.tile .k { display: block; font-size: 11px; text-transform: uppercase; letter-spacing: .07em;
  color: var(--text-muted); }
.tile .v { display: block; font-size: 30px; font-weight: 600; letter-spacing: -.02em; margin: 2px 0; }
.tile .c { display: block; font-size: 12px; color: var(--text-secondary); }
figure { margin: 20px 0 0; background: var(--surface-1); border: 1px solid var(--border);
  border-radius: 10px; padding: 16px 18px 8px; }
figcaption p { font-size: 12.5px; margin: 0 0 6px; color: var(--text-muted); }
.chart { width: 100%; height: auto; display: block; overflow: visible; }
.grid { stroke: var(--border); stroke-width: 1; }
.vgrid { stroke-dasharray: 2 4; }
.tick { fill: var(--text-muted); font-size: 11px; }
.tick.strong { fill: var(--text-secondary); font-size: 12px; font-weight: 600; }
.axis { fill: var(--text-secondary); font-size: 12px; }
.line { fill: none; stroke-width: 2; stroke-linejoin: round; }
.dot { stroke: var(--surface-1); stroke-width: 2; cursor: pointer; }
.direct { font-size: 12px; font-weight: 600; }
.direct.small { font-size: 11px; font-weight: 500; }
.bar { cursor: pointer; }
table { width: 100%; border-collapse: collapse; font-size: 12px; margin-top: 14px;
  background: var(--surface-1); border: 1px solid var(--border); border-radius: 8px; overflow: hidden; }
th, td { padding: 6px 8px; text-align: right; border-bottom: 1px solid var(--border); white-space: nowrap; }
th { background: var(--surface-2); color: var(--text-secondary); font-weight: 600; text-align: right;
  font-size: 11px; text-transform: uppercase; letter-spacing: .04em; }
td:first-child, th:first-child, td:nth-child(2), td:nth-child(3) { text-align: left; }
tr.c-copy td { border-left: 3px solid var(--s-copy); }
tr.c-rewrite td { border-left: 3px solid var(--s-rewrite); }
tr.c-clone td { border-left: 3px solid var(--s-clone); }
tr.c-clone_nodigest td { border-left: 3px solid var(--s-clone_nodigest); }
.scroll { overflow-x: auto; }
#tip { position: fixed; pointer-events: none; opacity: 0; transition: opacity .1s;
  background: var(--text-primary); color: var(--surface-1); font-size: 12px; line-height: 1.45;
  padding: 7px 9px; border-radius: 6px; max-width: 280px; z-index: 20; }
.note { background: var(--surface-1); border: 1px solid var(--border); border-left: 3px solid var(--s-clone_nodigest);
  border-radius: 8px; padding: 12px 14px; margin: 16px 0; font-size: 13.5px; }
.note b { color: var(--text-primary); }
.empty { color: var(--text-muted); font-style: italic; }
ul { color: var(--text-secondary); font-size: 14px; padding-left: 20px; }
code { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12.5px;
  background: var(--surface-2); padding: 1px 4px; border-radius: 3px; }
"""

JS = """
const tip = document.getElementById('tip');
for (const el of document.querySelectorAll('[data-tip]')) {
  const show = e => {
    tip.innerHTML = el.dataset.tip;
    tip.style.opacity = 1;
    const r = tip.getBoundingClientRect();
    const b = (e.target.getBoundingClientRect ? e.target : el).getBoundingClientRect();
    let x = (e.clientX || b.x + b.width / 2) + 14, y = (e.clientY || b.y) - r.height - 10;
    if (x + r.width > innerWidth - 8) x = innerWidth - r.width - 8;
    if (y < 8) y = (e.clientY || b.y) + 18;
    tip.style.left = x + 'px'; tip.style.top = y + 'px';
  };
  el.addEventListener('mouseenter', show);
  el.addEventListener('mousemove', show);
  el.addEventListener('focus', show);
  const hide = () => { tip.style.opacity = 0; };
  el.addEventListener('mouseleave', hide);
  el.addEventListener('blur', hide);
}
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("csv", nargs="+", metavar="CONFIG=FILE.csv",
                    help="one or more CONFIG=path.csv; CONFIG in " + ", ".join(SERIES))
    ap.add_argument("-o", "--out", default="docs/split-bench-comparison.html")
    ap.add_argument("--host", default="", help="what the numbers were measured on")
    args = ap.parse_args()

    rows = load(args.csv)
    if not rows:
        raise SystemExit("no measurement rows found")

    cold = collapse(rows, "fadvise")
    warm = collapse(rows, "none")
    tiles, head = hero(cold)

    def series_for(points, field):
        out = defaultdict(list)
        for (_, config), p in points.items():
            out[config].append({**p, "y": p[field]})
        return out

    body = [tiles]

    if head:
        speedup, alloc, zc, bl, best, written = head
        wr = "nothing measurable" if written is None else "{:,.0f}x less".format(written)
        body.append(
            '<div class="note"><b>Two different wins, and they do not arrive together.</b> '
            'On a {:.0f} GiB cold parent split {} ways, {} finishes in <b>{:.1f}s</b> against '
            '<b>{:.1f}s</b> for the rewrite, writes {}, and allocates <b>{:,.0f}x</b> less heap. '
            'Without reflink the picture is different: a byte copy still has to move the whole parent, so at '
            'this size it converges on the rewrite&rsquo;s wall clock because both are limited by the device '
            'rather than by CPU. Reflink is what turns the split from an O(data) operation into an O(index) '
            'one; skipping Digest.crc32 removes the last pass that reads the data at all.</div>'.format(
                zc["parent_mib"] / 1024, zc["children"], SERIES[best]["label"], zc["wall_s"],
                bl["wall_s"], wr, alloc))

    body.append("<h2>Wall clock</h2>")
    body.append(config_bars("Time to split, cold cache",
                            "Page cache dropped with posix_fadvise(DONTNEED) before every timed run, so no "
                            "configuration is reading from memory. Lower is better.",
                            cold, "wall_s", " s", lambda v: "{:.2f} s".format(v)))

    body.append("<h2>Bytes actually written</h2>")
    body.append(config_bars("New bytes on the platter",
                            "Child file length minus the part shared with the parent as copy-on-write extents. "
                            "A cloned range occupies no new space, so the children exist without the data being "
                            "duplicated at all &mdash; this is the column that decides whether a split needs "
                            "room for a second copy of the sstable.",
                            cold, "physical_mib", " MiB", lambda v: "{:,.0f} MiB".format(v)))

    # The size sweep and the row-shape comparison are different experiments; joining them with a line would
    # draw a trend across corpora that differ in shape, not in size.
    sweep = {k: v for k, v in cold.items() if "-v4096-" in k[0]}
    rowheavy = {k: v for k, v in cold.items() if "-v4096-" not in k[0]}

    body.append("<h2>Allocation</h2>")
    body.append(line_chart("Heap allocated by the splitting thread",
                           "The 4 KiB-row size sweep only. Log scale. The rewrite allocates in proportion to "
                           "the rows it materialises; no zero-copy variant materialises rows at all, so all of "
                           "them sit near the floor whatever the parent size.",
                           series_for(sweep, "alloc_mib"), "MiB allocated", log_y=True, unit=" MiB"))

    if rowheavy:
        body.append("<h2>Row shape and key type</h2>")
        body.append('<div class="note"><b>A composite UUID key did not widen the gap.</b> Both corpora below '
                    'hold exactly 1,048,576 partitions and 33,554,432 rows &mdash; identical row counts, so '
                    'this is a fair comparison &mdash; and differ only in schema: '
                    '<code>(k blob, c int)</code> against '
                    '<code>((k1 uuid, k2 uuid), c uuid)</code>. The rewrite allocated 19,807 MiB on the blob '
                    'shape and 19,945 MiB on the UUID shape, a 0.7% difference, and its wall clock actually '
                    'improved slightly per byte. What drives the rewrite&rsquo;s cost is the <em>number of '
                    'rows</em> it has to materialise, not how heavy each row&rsquo;s key is; heavier keys mean '
                    'fewer rows per byte, which works in the rewrite&rsquo;s favour. The interesting variable '
                    'here is not the key type but the row count: at 32 rows per partition the rewrite is CPU '
                    'bound again and loses 2x to a plain byte copy, where at 16 rows of 4 KiB it was device '
                    'bound and level with it.</div>')
        body.append(config_bars("Time to split, row-heavy corpora (32 rows x 256 B)",
                                "Same partition and row counts, different partition-key and clustering types.",
                                rowheavy, "wall_s", " s", lambda v: "{:.2f} s".format(v)))

    if warm:
        body.append("<h2>Warm cache</h2>")
        body.append(config_bars("Time to split, no eviction", "Same corpora with page cache left warm.",
                                warm, "wall_s", " s", lambda v: "{:.2f} s".format(v)))

    body.append("<h2>Every run</h2>")
    body.append('<div class="scroll">' + table(rows) + "</div>")

    body.append("<h2>How to read this</h2><ul>"
                "<li><b>existing rewrite</b> is the real <code>SSTableSplitter</code> "
                "(<code>sstablesplit</code>'s size-based rewrite) driven against a real "
                "<code>ColumnFamilyStore</code> and <code>LifecycleTransaction</code> &mdash; not a "
                "reimplementation. It is <em>not</em> "
                "<code>CompactionManager.performAnticompaction</code>'s three-writer rewrite; that path is "
                "not measured here.</li>"
                "<li><b>cloned</b> needs a filesystem implementing <code>FICLONERANGE</code> &mdash; xfs "
                "formatted <code>-m reflink=1</code>, or btrfs. Elsewhere the splitter self-demotes to a "
                "conventional transfer and produces a byte-identical child, so those rows would read like the "
                "byte-copy rows.</li>"
                "<li><b>w_amp</b> is child file length over parent length, so it stays near 1 even when "
                "cloning has made the children cost no space. Read <em>new bytes</em> for space.</li>"
                "<li><b>dead prefix</b> is the price of cutting on the compression chunk grid; <b>head pad</b> "
                "is the price of clone-range alignment.</li>"
                "<li><b>alloc</b> is the calling thread only; work on Cassandra's shared executors is counted "
                "for no configuration.</li>"
                "<li>Wall clock covers the split call alone. Corpus construction, hard linking, eviction and "
                "verification are all outside it, and every run verified that the children carry exactly the "
                "parent's partition keys before its number was kept.</li></ul>")

    generated = f" &middot; {html.escape(args.host)}" if args.host else ""
    page = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Zero-copy SSTable split vs the existing rewrite</title>
<style>{CSS}</style></head>
<body class="viz-root">
<div class="wrap">
<h1>Zero-copy SSTable split vs the existing rewrite</h1>
<p class="lede">{len(rows)} measured splits from <code>LargeSSTableSplitBench</code>{generated}.
Orange is the full rewrite that ships today; the others are the zero-copy split with copy-on-write extent sharing and Digest.crc32 turned on and off.</p>
{''.join(body)}
</div>
<div id="tip" role="status" aria-live="polite"></div>
<script>{JS}</script>
</body></html>
"""
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w") as handle:
        handle.write(page)
    print(f"wrote {args.out} ({len(rows)} runs, {len(cold) // 2} cold size points, "
          f"{len(warm) // 2} warm)")


if __name__ == "__main__":
    main()
