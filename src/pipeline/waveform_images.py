"""Per-event narrow waveform images for the Waveform Viewer.

Canonical layout::

    data/waveforms/<event_id>/forward_narrow.jpg
    data/waveforms/<event_id>/cavity_narrow.jpg
    data/waveforms/<event_id>/reflect_narrow.jpg

The web viewer reads only this directory.  The monitoring pipeline copies the
already-rendered ``data/graph_only`` images here (they are produced from the
raw CSV at 1 us, before the event id exists); events without those images are
rendered from the merged parquet (2 us for most 2025+ events).
"""

from __future__ import annotations

import logging
import shutil
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Mapping

logger = logging.getLogger(__name__)

SCOPES = ("forward", "cavity", "reflect")
SCOPE_PREFIX = {"forward": "Forward", "cavity": "Cavity", "reflect": "Reflect"}
SCOPE_INDEX = {1: "forward", 2: "cavity", 3: "reflect"}
NARROW_MS = 1.0

# The raw scope CSVs split the interlocks: forward has the 10 INT_/RDY_KSU
# channels, cavity has the 15 CM channels, and reflect has none.  The merged
# parquet contains the union (25), so each panel shows only its own set.
FORWARD_DIGITAL = (
    "INT_MIS1_IC_d", "INT_MIS2_IC_d", "INT_IN_FC3_d", "INT_IC_FC2_d",
    "INT_IC_FC1_d", "RDY_KSU3_IC_d", "RDY_KSU2_IC_d", "RDY_KSU1_IC_d",
    "INT_PSI2_IC_d", "INT_PSI1_IC_d",
)


def _scope_digital_cols(scope: str, digital_cols: list[str]) -> list[str]:
    if scope == "forward":
        wanted = set(FORWARD_DIGITAL)
        return [c for c in digital_cols if c in wanted]
    if scope == "cavity":
        wanted = set(FORWARD_DIGITAL)
        return [c for c in digital_cols if c not in wanted]
    return []

# Matplotlib is not thread-safe.  The monitoring pipeline renders in one
# worker and the backfill script is sequential, but keep a guard anyway.
_GEN_LOCK = threading.Lock()

_THEME = {
    "BG": "#0f1117",
    "PANEL": "#1a1d27",
    "GRID": "#2a2d3a",
    "TXT": "#e0e0e0",
    "TITLE": "#ffffff",
}
_ANALOG_COLORS = {
    "BeamCurrent": "#FFE033",
    "SRF1": "#00D4FF",
    "SRF2": "#FF1E6B",
    "SRF3": "#96D800",
}
_DIGITAL_PALETTE = [
    "#7986CB", "#4DB6AC", "#FFB74D", "#E57373", "#BA68C8", "#4DD0E1",
    "#AED581", "#F06292", "#64B5F6", "#A1887F", "#90A4AE", "#FFF176",
    "#80CBC4", "#CE93D8", "#FFCC02", "#80DEEA",
]

_KST = timezone(timedelta(hours=9))


def project_root() -> Path:
    """Repository root (…/SRF_Postmortem_WebDB)."""
    return Path(__file__).resolve().parents[2]


def waveforms_dir() -> Path:
    return project_root() / "data" / "waveforms"


def image_filename(scope: str) -> str:
    return f"{scope}_narrow.jpg"


def image_path(event_id: str, scope: str) -> Path:
    return waveforms_dir() / event_id / image_filename(scope)


def existing_images(event_id: str) -> dict[str, Path]:
    """Return the scope -> path map for images that exist and are non-empty."""
    found: dict[str, Path] = {}
    for scope in SCOPES:
        path = image_path(event_id, scope)
        try:
            if path.is_file() and path.stat().st_size > 0:
                found[scope] = path
        except OSError:
            continue
    return found


def copy_image_set(
    event_id: str,
    sources: Mapping[str, Path | str],
    *,
    force: bool = False,
) -> bool:
    """Copy scope images (NAS or graph_only) into the canonical layout.

    Only missing images are copied unless the destination is older than the
    source.  Returns True when all three scopes are present afterwards.
    """
    dest_dir = waveforms_dir() / event_id
    dest_dir.mkdir(parents=True, exist_ok=True)
    for scope in SCOPES:
        src = sources.get(scope)
        if not src:
            continue
        src_path = Path(src)
        if not src_path.is_file():
            continue
        dest = dest_dir / image_filename(scope)
        try:
            if not force and dest.is_file() and dest.stat().st_mtime >= src_path.stat().st_mtime:
                continue
        except OSError:
            pass
        shutil.copy2(src_path, dest)
    return len(existing_images(event_id)) == len(SCOPES)


def generate_image_set(
    event_id: str,
    merged_path: Path | str,
    *,
    force: bool = False,
) -> bool:
    """Render the three narrow scope images from a merged parquet file.

    The merged parquet is already decimated, so the narrow ±1 ms window has
    about 1,000 points for 2025+ events (2 us sampling).  Returns True when all
    three images exist afterwards.
    """
    if not force and len(existing_images(event_id)) == len(SCOPES):
        return True

    merged_path = Path(merged_path)
    if not merged_path.is_file():
        logger.warning("waveform images: merged parquet missing for %s: %s", event_id, merged_path)
        return False

    dest_dir = waveforms_dir() / event_id
    dest_dir.mkdir(parents=True, exist_ok=True)

    try:
        with _GEN_LOCK:
            _render_merged(merged_path, dest_dir, event_id)
    except Exception:
        logger.exception("waveform images: render failed for %s", event_id)
        return False

    return len(existing_images(event_id)) == len(SCOPES)


def ensure_image_set(
    event_id: str,
    merged_path: Path | str | None = None,
    sources: Mapping[str, Path | str] | None = None,
    *,
    force: bool = False,
) -> bool:
    """Best-effort: prefer already-rendered scope images, else render parquet."""
    if not force and len(existing_images(event_id)) == len(SCOPES):
        return True

    if sources:
        copy_image_set(event_id, sources)
        if not force and len(existing_images(event_id)) == len(SCOPES):
            return True

    if merged_path:
        return generate_image_set(event_id, merged_path, force=force)

    return len(existing_images(event_id)) > 0


def event_id_to_kst(ts: str) -> datetime | None:
    """Parse a DB timestamp (naive KST) into an aware KST datetime."""
    try:
        parsed = datetime.fromisoformat(ts)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=_KST)
    return parsed.astimezone(_KST)


# ── Renderer ─────────────────────────────────────────────────

def _render_merged(merged_path: Path, dest_dir: Path, event_id: str) -> None:
    """Render one narrow image per scope from the merged parquet."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
    import pyarrow.parquet as pq

    table = pq.read_table(merged_path)
    names = table.schema.names

    if "t_rel_s" not in names:
        raise ValueError(f"t_rel_s missing in {merged_path}")

    t = np.asarray(table.column("t_rel_s").to_pylist(), dtype=float)
    mask = (t >= -NARROW_MS / 1000.0) & (t <= NARROW_MS / 1000.0)
    if not mask.any():
        raise ValueError(f"no samples in narrow window: {merged_path}")

    digital_cols = [c for c in names if c.endswith("_d")]

    # Title time: prefer the parquet's event_timestamp, else the event id.
    title_time = ""
    if "event_timestamp" in names:
        try:
            ts = table.column("event_timestamp").to_pylist()[0]
            if isinstance(ts, datetime):
                title_time = ts.astimezone(_KST).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            title_time = ""
    if not title_time:
        parsed = event_id_to_kst(
            f"{event_id[:4]}-{event_id[4:6]}-{event_id[6:8]}T"
            f"{event_id[9:11]}:{event_id[11:13]}:{event_id[13:15]}"
        )
        title_time = parsed.strftime("%Y-%m-%d %H:%M:%S") if parsed else event_id

    bg = _THEME["BG"]
    panel = _THEME["PANEL"]
    grid = _THEME["GRID"]
    txt = _THEME["TXT"]
    title = _THEME["TITLE"]

    tv_ms = t[mask] * 1000.0
    x_min = tv_ms[0] if len(tv_ms) else -NARROW_MS
    x_max = tv_ms[-1] if len(tv_ms) else NARROW_MS

    with plt.rc_context({
        "font.family": "DejaVu Sans",
        "font.size": 10,
        "axes.facecolor": panel,
        "figure.facecolor": bg,
        "axes.edgecolor": grid,
        "axes.labelcolor": txt,
        "xtick.color": txt,
        "ytick.color": txt,
        "text.color": txt,
        "grid.color": grid,
        "grid.linestyle": "--",
        "grid.linewidth": 0.6,
        "grid.alpha": 0.8,
    }):
        for scope in SCOPES:
            prefix = SCOPE_PREFIX[scope]
            specs = [
                ("BeamCurrent_v", "BeamCurrent", lambda v: v / 2.0 * 7.0),
                (f"{prefix}_SRF1_v", "SRF1", lambda v: v * 35.0),
                (f"{prefix}_SRF2_v", "SRF2", lambda v: v * 35.0),
                (f"{prefix}_SRF3_v", "SRF3", lambda v: v * 35.0),
            ]
            specs = [s for s in specs if s[0] in names]
            if not specs:
                logger.warning("waveform images: no analog columns for %s/%s", event_id, scope)
                continue

            scope_digital = _scope_digital_cols(scope, digital_cols)

            # 2448x1374 px at 150 dpi -> ~1.3x the previous width, 16:9.
            fig = plt.figure(figsize=(19.5, 9.3))
            fig.patch.set_facecolor(bg)
            if scope_digital:
                # Digital on top, analog below (same order as the raw scope CSV).
                # Size the digital panel so the 12pt label boxes almost touch
                # (~30 px pitch at 150 dpi).
                _axes_px = 9.3 * 150 * 0.87
                _f = min(0.75, 30.0 * (len(scope_digital) + 0.8) / _axes_px)
                digital_h = max(0.8, 4.0 * _f / (1.0 - _f))
                gs = fig.add_gridspec(2, 1, height_ratios=[digital_h, 4], hspace=0.08)
                ax_d = fig.add_subplot(gs[0])
                ax_a = fig.add_subplot(gs[1], sharex=ax_d)
                ax_a.set_facecolor(panel)
                ax_d.set_facecolor(panel)
                ax_d.tick_params(labelbottom=False)
            else:
                ax_a = fig.add_subplot(1, 1, 1)
                ax_d = None
                ax_a.set_facecolor(panel)

            # ── Analog: all traces share one normalised axis ──
            all_norm: list = []
            for name, key, _scale in specs:
                values = np.asarray(table.column(name).to_pylist(), dtype=float)
                values = np.nan_to_num(values, nan=0.0, posinf=0.0, neginf=0.0)
                window = values[mask]
                # Baseline-relative normalisation: the pre-event level (first
                # 10% of the narrow window) becomes 1.0.  Every trace is drawn
                # on the same axis, so overlapping shows which signal reacts
                # first.
                n = max(1, len(window) // 10)
                baseline = float(np.nanmedian(window[:n])) if len(window) else 1.0
                if not np.isfinite(baseline) or abs(baseline) < 1e-6:
                    baseline = float(np.nanmax(np.abs(window))) or 1.0
                norm = window / baseline
                all_norm.append(norm)
                color = _ANALOG_COLORS[key]
                label = name.replace("_v", "")
                ax_a.plot(tv_ms, norm, color=color, lw=4.0, alpha=0.9, label=label)

            if all_norm:
                merged = np.concatenate([a for a in all_norm if len(a)])
                peak = float(np.nanmax(merged)) if len(merged) else 1.0
                lo = float(np.nanmin(merged)) if len(merged) else 0.0
            else:
                peak, lo = 1.0, 0.0
            top = float(np.clip(peak, 1.15, 2.0))
            clipped = bool(all_norm) and (peak > top + 1e-9)
            ax_a.set_ylim(min(lo, 0.0) - 0.05, top + 0.05)
            ax_a.axhline(1.0, color="#8b949e", lw=0.8, ls=":", alpha=0.8)
            ax_a.axhline(0.0, color="#8b949e", lw=0.8, ls=":", alpha=0.5)
            ax_a.axvline(0.0, color="#8b949e", lw=0.8, ls="--", alpha=0.55)
            ax_a.annotate("start = 1.0", (x_min, 1.0), xytext=(4, 5),
                          textcoords="offset points", fontsize=8, color="#c9d1d9")
            if clipped:
                ax_a.annotate(f"peaks > {top:.2f} clipped", (x_max, top),
                              xytext=(-4, -12), textcoords="offset points",
                              fontsize=7.5, color="#8b949e", ha="right")
            ax_a.set_ylabel("Normalized (start=1)")
            ax_a.grid(True)
            for spine in ax_a.spines.values():
                spine.set_edgecolor(grid)
            legend = ax_a.legend(
                loc="upper right", frameon=True,
                handlelength=2.0, handleheight=1.2, labelspacing=0.5,
            )
            legend.get_frame().set_facecolor(panel)
            legend.get_frame().set_edgecolor(grid)
            legend.get_frame().set_alpha(0.9)
            for text in legend.get_texts():
                text.set_color("white")

            # ── Digital: only the scope's own interlock set ──
            ndig = len(scope_digital)
            if ax_d is not None and ndig:
                gap_d, amp_d = 1.0, 0.6
                for i, name in enumerate(scope_digital):
                    values = np.asarray(table.column(name).to_pylist(), dtype=float)
                    values = np.nan_to_num(values, nan=0.0, posinf=0.0, neginf=0.0)
                    y_base = (ndig - 1 - i) * gap_d
                    color = _DIGITAL_PALETTE[i % len(_DIGITAL_PALETTE)]
                    ax_d.axhspan(y_base - 0.05, y_base + amp_d + 0.05,
                                 alpha=0.03, color=color, zorder=0)
                    ax_d.step(tv_ms, values[mask] * amp_d + y_base, where="post",
                              color=color, lw=1.0, alpha=0.9)
                    ax_d.text(
                        x_min, y_base + amp_d / 2, name.replace("_d", ""), fontsize=12.0,
                        color=color, ha="left", va="center",
                        bbox=dict(boxstyle="round,pad=0.1", fc=panel, ec=color,
                                  alpha=0.72, lw=0.4),
                    )
                ax_d.set_ylim(-0.1, (ndig - 1) * gap_d + amp_d + 0.1)
                ax_d.set_yticks([])
                ax_d.axvline(0.0, color="#8b949e", lw=0.8, ls="--", alpha=0.55)
                ax_d.grid(True, axis="x")
                for spine in ax_d.spines.values():
                    spine.set_edgecolor(grid)

            ax_a.set_xlabel("Time (ms)")
            ax_a.set_xlim(x_min, x_max)
            fig.suptitle(
                f"SRF Postmortem | {prefix} | -{NARROW_MS:.1f}~+{NARROW_MS:.1f}ms | {title_time}",
                fontsize=13, color=title, fontweight="bold", y=0.975,
            )
            fig.subplots_adjust(left=0.06, right=0.84, top=0.93, bottom=0.06)
            out = dest_dir / image_filename(scope)
            fig.savefig(str(out), dpi=150, bbox_inches="tight", facecolor=bg)
            plt.close(fig)
            logger.info("waveform images: wrote %s", out)
