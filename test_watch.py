#!/usr/bin/env python3
"""
SRF Postmortem — Watchdog only, generate graphs, NO pipeline.
3개 폴더(W:\, X:\, Y:\) CSV 감지 → 그래프 이미지 2개씩만 생성.
출처: main_v0.4_다크모드.py
"""
import os, sys, time, threading, datetime, io, json
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import queue

KST = datetime.timezone(datetime.timedelta(hours=9))
BG_COLOR = '#0f1117'
PANEL_COLOR = '#1a1d27'
GRID_COLOR = '#2a2d3a'
TEXT_COLOR = '#e0e0e0'
TITLE_COLOR = '#ffffff'
ANALOG_COLORS = {'B':'#FFE033','C':'#00D4FF','D':'#FF1E6B','E':'#96D800'}
DIGITAL_PALETTE = [
    '#7986CB','#4DB6AC','#FFB74D','#E57373','#BA68C8','#4DD0E1','#AED581','#F06292',
    '#64B5F6','#A1887F','#90A4AE','#FFF176','#80CBC4','#CE93D8','#FFCC02','#80DEEA',
]

FOLDERS = [r"W:\", r"X:\", r"Y:\"]
OUTPUT_BASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "graphs")
os.makedirs(OUTPUT_BASE, exist_ok=True)

csv_queue = queue.Queue()
processed = set()
observers = []

def ts(msg):
    t = datetime.datetime.now(KST).strftime("%H:%M:%S")
    print(f"[{t}] {msg}", flush=True)

def wait_for_file(fp, timeout=120):
    start = time.time()
    last = -1
    while True:
        try:
            with open(fp, 'rb') as f: f.read(1)
            sz = os.path.getsize(fp)
        except: sz = -1
        if sz == last and sz > 0: return sz
        last = sz
        if time.time() - start > timeout: return max(sz, 0)
        time.sleep(2.0)

def plot_combined(df_data, time_col, savepath, tmin, tmax, labels, df_full=None):
    plt.rcParams.update({
        'font.family':'DejaVu Sans','font.size':10,'axes.titlesize':12,'axes.labelsize':11,
        'xtick.labelsize':10,'ytick.labelsize':10,'figure.facecolor':BG_COLOR,
        'axes.facecolor':PANEL_COLOR,'axes.edgecolor':GRID_COLOR,'axes.labelcolor':TEXT_COLOR,
        'xtick.color':TEXT_COLOR,'ytick.color':TEXT_COLOR,'text.color':TEXT_COLOR,
        'grid.color':GRID_COLOR,'grid.linestyle':'--','grid.linewidth':0.6,'grid.alpha':0.8,
        'legend.facecolor':'#1e2130','legend.edgecolor':'#3a3d50','legend.framealpha':0.9,'legend.fontsize':9,
    })
    fig, ax = plt.subplots(figsize=(15,8))
    fig.patch.set_facecolor(BG_COLOR)
    b_col, c_col, d_col, e_col = labels[1], labels[2], labels[3], labels[4]
    digital_cols = df_data.columns[6:22]
    analog_cols = [c for c in df_data.columns if c not in digital_cols and c != time_col]
    tv = df_data[time_col].values * 1000
    xleft = tmin * 1000
    dinfo = df_full if df_full is not None else df_data
    col_map = {b_col:('B',lambda v:v/2.0*7), c_col:('C',lambda v:v*35), d_col:('D',lambda v:v*35), e_col:('E',lambda v:v*35)}
    for col in analog_cols:
        if col not in col_map: continue
        key, fn = col_map[col]
        c = ANALOG_COLORS[key]
        sc = fn(df_data[col].values)
        ax.plot(tv, sc, color=c, linewidth=2.2, alpha=0.85, label=col)
        ax.plot(tv, sc, color=c, linewidth=0.7, alpha=1.0, label='_nolegend_')
        iy = float(fn(dinfo[col].iloc[0]))
        ax.plot(xleft, iy, marker='<', markersize=11, markerfacecolor=c, markeredgecolor='white', markeredgewidth=1.2, zorder=6, clip_on=False, label='_nolegend_')
        ax.annotate(f'{dinfo[col].iloc[0]:.3f}', xy=(xleft,iy), xytext=(6,0), textcoords='offset points', fontsize=7.5, color=c, alpha=0.85, va='center')
    gap, sty = 1.2, 38
    for i, col in enumerate(digital_cols):
        y = df_data[col].fillna(0).values
        yb = sty - i * gap
        dc = DIGITAL_PALETTE[i % len(DIGITAL_PALETTE)]
        ax.axhspan(yb-0.05, yb+1.1, alpha=0.04, color=dc, zorder=0)
        ax.step(tv, y + yb, where='post', color=dc, linewidth=1.1, alpha=0.9, label='_nolegend_')
        ax.text(tv[0], yb+0.25, col, fontsize=8, color=dc, ha='left', va='bottom',
                bbox=dict(boxstyle='round,pad=0.15', facecolor=PANEL_COLOR, edgecolor=dc, alpha=0.7, linewidth=0.6))
    ax.set_xlabel("Time (ms)", color=TEXT_COLOR)
    ax.set_ylabel("Amplitude (a.u.)", color=TEXT_COLOR)
    now_s = datetime.datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    ax.set_title(f"SRF Postmortem Waveform\n{tmin*1000:.1f}~{tmax*1000:+.1f}ms {now_s}", fontsize=12, color=TITLE_COLOR, fontweight='bold', pad=12)
    ax.set_ylim(df_data[analog_cols].min().min()-1 if not df_data[analog_cols].empty else -1, 41)
    ax.set_xlim(tv[0], tv[-1])
    ax.grid(True)
    for s in ax.spines.values(): s.set_edgecolor(GRID_COLOR)
    leg = ax.legend(loc='upper left', bbox_to_anchor=(1.0,1), frameon=True, handlelength=2.0)
    for t in leg.get_texts(): t.set_color(TEXT_COLOR)
    plt.tight_layout(pad=1.8)
    fig.subplots_adjust(right=0.82)
    plt.savefig(savepath, dpi=150, bbox_inches='tight', facecolor=BG_COLOR)
    plt.close()
    plt.rcParams.update(plt.rcParamsDefault)

def process_csv(fp):
    ts(f"Processing {os.path.basename(fp)}...")
    sz = wait_for_file(fp)
    if sz <= 0:
        ts(f"SKIP: {os.path.basename(fp)} not ready")
        return False
    try:
        with open(fp, 'r', encoding='utf-8') as f: lines = f.readlines()
    except:
        with open(fp, 'r', encoding='cp949') as f: lines = f.readlines()
    labels = lines[19].strip().split(',')
    df = pd.read_csv(io.StringIO(''.join(lines[21:])), header=None)
    if len(labels) > 6: labels.pop(6)
    if df.shape[1] > 6: df.drop(columns=[6], inplace=True)
    df.columns = labels
    tc = labels[0]
    # Beam current filter
    try:
        if float(df.iloc[0,1]) < 3:
            ts(f"Beam low, skip: {os.path.basename(fp)}")
            return False
    except: pass
    stem = os.path.splitext(os.path.basename(fp))[0]
    # Find which watch folder
    folder_name = "unknown"
    for fi, fd in enumerate(FOLDERS):
        if os.path.dirname(fp).rstrip('\\').upper() == fd.rstrip('\\').upper():
            folder_name = f"scope{fi+1}"
            break
    outdir = os.path.join(OUTPUT_BASE, folder_name)
    os.makedirs(outdir, exist_ok=True)
    for tmin, tmax, suffix in [(-0.05, 0.05, "wide"), (-0.001, 0.001, "narrow")]:
        mask = (df[tc] >= tmin) & (df[tc] <= tmax)
        dff = df[mask].reset_index(drop=True)
        sp = os.path.join(outdir, f"{stem}_{suffix}.jpg")
        plot_combined(dff, tc, sp, tmin, tmax, labels, df_full=df)
        ts(f"  Graph saved: {os.path.basename(sp)}")
    return True

def csv_worker():
    while True:
        path = csv_queue.get()
        if path is None: break
        if path in processed:
            csv_queue.task_done()
            continue
        processed.add(path)
        process_csv(path)
        csv_queue.task_done()

def start_watchers():
    global observers
    class Handler(FileSystemEventHandler):
        def on_created(self, ev):
            if not ev.is_directory and ev.src_path.lower().endswith('.csv'):
                csv_queue.put(ev.src_path)
        def on_modified(self, ev):
            if not ev.is_directory and ev.src_path.lower().endswith('.csv'):
                csv_queue.put(ev.src_path)
    for obs in observers:
        try: obs.stop()
        except: pass
    observers = []
    for folder in FOLDERS:
        if not os.path.isdir(folder):
            ts(f"Folder NOT FOUND: {folder}")
            continue
        ts(f"Watching: {folder}")
        handler = Handler()
        obs = Observer()
        obs.schedule(handler, folder, recursive=False)
        obs.start()
        observers.append(obs)

def main():
    ts("=== SRF Test Watch: Graph-only Mode ===")
    ts(f"Folders: {FOLDERS}")
    ts(f"Output: {OUTPUT_BASE}")
    threading.Thread(target=csv_worker, daemon=True).start()
    start_watchers()
    ts("Ready. Waiting for CSV files...")
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        ts("Stopped")
        for obs in observers:
            try: obs.stop()
            except: pass

if __name__ == "__main__":
    main()
