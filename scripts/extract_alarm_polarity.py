"""Extract alarm polarity from the CSS OPI rule expressions.

For every widget that uses a PV in a `rules` block, read the color-condition
expressions (red = alarm, green = normal) and the widget's default background
color.  The result is a per-PV mapping used by the alarm monitor.

Outputs:
  data/alarm/alarm_polarity.json
  data/alarm/alarm_polarity.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import xml.etree.ElementTree as ET
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

OPI_FILES = [
    ("KSU1", "CSS/KSU/KSU1_Status.opi"),
    ("KSU2", "CSS/KSU/KSU2_Status.opi"),
    ("KSU3", "CSS/KSU/KSU3_Status.opi"),
    ("CRM1", "CSS/cryo_module/Readychain1.opi"),
    ("CRM2", "CSS/cryo_module/Readychain2.opi"),
    ("CRM3", "CSS/cryo_module/Readychain3.opi"),
    ("CRG", "CSS/cryogenic/CRG_Status.opi"),
]


def _color(el):
    if el is None:
        return None
    c = el.find("value/color")
    if c is None:
        c = el.find("color")
    if c is None:
        c = next(el.iter("color"), None)
    if c is None:
        return None
    try:
        return (int(c.get("red", -1)), int(c.get("green", -1)), int(c.get("blue", -1)))
    except (TypeError, ValueError):
        return None


def _kind(rgb):
    if not rgb:
        return "unknown"
    r, g, b = rgb
    if r >= 200 and g <= 60 and b <= 60:
        return "alarm"
    # Bright green and the dark green "off" shade used by LED widgets
    # (0,100,0) both mean the operator sees a normal state.
    if g >= 80 and g > r + 30 and g > b + 30:
        return "normal"
    return "unknown"


def parse_opi(path: Path):
    root = ET.parse(path).getroot()
    out = {}
    for w in root.iter("widget"):
        rules = w.find("rules")
        if rules is None:
            continue
        # LED widgets express their state through off_color (the rule target);
        # their background_color is just the panel behind the bulb.  Other
        # widgets (labels/rectangles) use background_color directly.
        props = {r.get("prop_id") for r in rules.findall("rule")}
        if "off_color" in props:
            default_rgb = _color(w.find("off_color"))
        else:
            default_rgb = _color(w.find("background_color"))
        for rule in rules.findall("rule"):
            pv = None
            for p in rule.iter("pv"):
                if p.get("trig") == "true" or pv is None:
                    pv = (p.text or "").strip()
            if not pv:
                continue
            entry = out.setdefault(pv, {
                "alarm_expr": [], "normal_expr": [],
                "default_state": [], "source": path.name,
            })
            for exp in rule.findall("exp"):
                expr = (exp.get("bool_exp") or "").strip()
                if not expr:
                    continue
                kind = _kind(_color(exp))
                if kind == "alarm":
                    if expr not in entry["alarm_expr"]:
                        entry["alarm_expr"].append(expr)
                elif kind == "normal":
                    if expr not in entry["normal_expr"]:
                        entry["normal_expr"].append(expr)
            ds = _kind(default_rgb)
            if ds not in entry["default_state"]:
                entry["default_state"].append(ds)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--opi-root", default=str(PROJECT_ROOT))
    ap.add_argument("--out-json", default=str(PROJECT_ROOT / "data/alarm/alarm_polarity.json"))
    ap.add_argument("--out-csv", default=str(PROJECT_ROOT / "data/alarm/alarm_polarity.csv"))
    args = ap.parse_args()

    root = Path(args.opi_root)
    merged = {}
    for tag, rel in OPI_FILES:
        path = root / rel
        if not path.exists():
            print(f"WARN missing {path}")
            continue
        parsed = parse_opi(path)
        for pv, e in parsed.items():
            d = merged.setdefault(pv, {
                "alarm_expr": [], "normal_expr": [],
                "default_state": [], "sources": [],
            })
            for x in e["alarm_expr"]:
                if x not in d["alarm_expr"]:
                    d["alarm_expr"].append(x)
            for x in e["normal_expr"]:
                if x not in d["normal_expr"]:
                    d["normal_expr"].append(x)
            for x in e["default_state"]:
                if x not in d["default_state"]:
                    d["default_state"].append(x)
            if tag not in d["sources"]:
                d["sources"].append(tag)

    rows = []
    for pv, d in sorted(merged.items()):
        # Default state: only trust it when all widgets agree.
        ds = d["default_state"]
        default_state = ds[0] if len(set(ds)) == 1 else "unknown"
        if not default_state:
            default_state = "unknown"

        # Multi-state / summary PVs are marked complex.
        is_complex = pv.endswith(":ACTUAL_STATE") or len(set(d["alarm_expr"])) > 2
        state_mode = "complex" if is_complex else ("expr" if (d["alarm_expr"] or d["normal_expr"]) else "default")

        enabled = 0 if pv.endswith(":ACTUAL_STATE") else 1
        rows.append({
            "pv": pv,
            "alarm_expr": json.dumps(d["alarm_expr"], ensure_ascii=False),
            "normal_expr": json.dumps(d["normal_expr"], ensure_ascii=False),
            "default_state": default_state,
            "state_mode": state_mode,
            "enabled": enabled,
            "sources": ";".join(d["sources"]),
        })

    Path(args.out_json).parent.mkdir(parents=True, exist_ok=True)
    json.dump(rows, open(args.out_json, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    with open(args.out_csv, "w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["pv", "alarm_expr", "normal_expr",
                                          "default_state", "state_mode", "enabled", "sources"])
        w.writeheader()
        w.writerows(rows)

    from collections import Counter
    print("PVs with rules:", len(rows))
    print("state_mode:", dict(Counter(r["state_mode"] for r in rows)))
    print("default_state:", dict(Counter(r["default_state"] for r in rows)))
    print("enabled:", dict(Counter(r["enabled"] for r in rows)))
    print("no red expr:", sum(1 for r in rows if r["alarm_expr"] == "[]"))
    print("wrote", args.out_json, args.out_csv)


if __name__ == "__main__":
    main()
