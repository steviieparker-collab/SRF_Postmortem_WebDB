/**
 * SRF Postmortem Viewer — Chart Rendering
 *
 * Fetches real waveform data from the parquet file via API and renders an
 * interactive Plotly chart as a vertical stack of panels sharing one X axis:
 *
 *   1. BeamCurrent + Forward_SRF1..3
 *   2. BeamCurrent + Cavity_SRF1..3
 *   3. BeamCurrent + Reflect_SRF1..3
 *   4. all digital interlocks (offset-stacked, custom HTML legend)
 *
 * Each analog panel auto-scales its own Y axis and is drawn with a full
 * border box. Digital channels keep the fixed offset layout and the custom
 * HTML legend box, since Plotly's legend cannot express per-channel offsets.
 */

let currentView = '1ms';
let waveformData = null;
let currentFilename = '';

// Last synthetic inputs, so a view-range change can re-render the fallback
// chart without the caller having to pass the metrics again.
let syntheticAnalog = null;
let syntheticDigital = null;

// ── Color palettes ────────────────────────────────────
const ANALOG_COLORS = [
    '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728',
    '#9467bd', '#8c564b', '#e377c2', '#7f7f7f',
    '#bcbd22', '#17becf',
];

// Thicken every trace relative to the configured/base widths.
const LINE_WIDTH_SCALE = 1.5;
const DIGITAL_LINE_WIDTH = 1.0;

// Per-signal waveform style from server config (analogStyles).
// Returns { color, width, dash } for a signal name, falling back to
// index-based palette, then global ANALOG_STYLE_DEFAULTS.
const ANALOG_STYLE_DEFAULTS = { color: '#AAAAAA', width: 1.5, dash: 'solid' };
function analogLineStyle(name, idx) {
    // Browser waveform uses label keys (e.g. "BeamCurrent", no _v suffix) while
    // config analog_styles uses raw column names (e.g. "BeamCurrent_v").
    // Try exact, then append _v, then strip _v for lookup.
    let st = (analogStyles && analogStyles[name]) || null;
    if (!st && analogStyles && (name + '_v') in analogStyles) st = analogStyles[name + '_v'];
    if (!st && analogStyles && name.endsWith('_v') && (name.slice(0, -2)) in analogStyles) st = analogStyles[name.slice(0, -2)];
    if (st) {
        let dash = st.linestyle || 'solid';
        if (dash === ':' || dash === 'dotted') dash = 'dot';
        else if (dash === '--' || dash === 'dashed') dash = 'dash';
        else if (dash === '-.') dash = 'dashdot';
        // longdash / longdashdot / solid pass through as-is for Plotly
        return {
            color: st.color || ANALOG_STYLE_DEFAULTS.color,
            width: (st.linewidth != null ? Number(st.linewidth) : 2.2) * LINE_WIDTH_SCALE,
            dash: dash,
        };
    }
    // Fallback: palette by index
    return {
        color: ANALOG_COLORS[(idx || 0) % ANALOG_COLORS.length],
        width: 1.5 * LINE_WIDTH_SCALE,
        dash: 'solid',
    };
}

// Stable color slot per analog channel: BeamCurrent keeps one color across all
// three panels, and each SRF channel gets its own instead of colliding by
// per-panel index.
const ANALOG_CHANNEL_ORDER = [
    'BeamCurrent',
    'Forward_SRF1', 'Forward_SRF2', 'Forward_SRF3',
    'Cavity_SRF1', 'Cavity_SRF2', 'Cavity_SRF3',
    'Reflect_SRF1', 'Reflect_SRF2', 'Reflect_SRF3',
];
const unknownAnalogSlots = new Map();
function analogColorIndex(name) {
    const known = ANALOG_CHANNEL_ORDER.indexOf(name);
    if (known >= 0) return known;
    if (!unknownAnalogSlots.has(name)) {
        unknownAnalogSlots.set(name, ANALOG_CHANNEL_ORDER.length + unknownAnalogSlots.size);
    }
    return unknownAnalogSlots.get(name);
}

const DIGITAL_COLORS = [
    '#ff6b6b',  // red (0°)
    '#ff922b',  // orange (30°)
    '#ffd43b',  // yellow (45°)
    '#69db7c',  // green (120°)
    '#20c997',  // teal (150°)
    '#22b8cf',  // cyan (180°)
    '#339af0',  // blue (210°)
    '#5c7cfa',  // indigo (225°)
    '#9775fa',  // purple (270°)
    '#da77f2',  // magenta (300°)
    '#f783ac',  // pink (330°)
    '#fab005',  // gold (45°)
    '#94d82d',  // lime (90°)
    '#0ca678',  // dark teal (165°)
    '#4dabf7',  // sky blue (195°)
    '#cc5de8',  // plum (285°)
];

const MAX_DIGITAL_CHANNELS = 25;
const DIGITAL_CHANNEL_GAP = 1.1;

// Digital channels pinned to the BOTTOM of the stack, listed top -> bottom.
// Channels not named here stay above them, in the order the API returns
// (alphabetical). Names are API labels: raw column name minus its _d suffix.
const DIGITAL_BOTTOM_ORDER = [
    'RDY_KSU1_IC', 'RDY_KSU2_IC', 'RDY_KSU3_IC',
    'INT_MIS1_IC', 'INT_MIS2_IC', 'INT_PSI1_IC', 'INT_PSI2_IC',
];

// ── Panel definition & geometry ───────────────────────
// Display order, top to bottom. BeamCurrent is repeated in every analog panel.
const ANALOG_PANELS = [
    { title: 'Forward', channels: ['BeamCurrent', 'Forward_SRF1', 'Forward_SRF2', 'Forward_SRF3'] },
    { title: 'Cavity',  channels: ['BeamCurrent', 'Cavity_SRF1', 'Cavity_SRF2', 'Cavity_SRF3'] },
    { title: 'Reflect', channels: ['BeamCurrent', 'Reflect_SRF1', 'Reflect_SRF2', 'Reflect_SRF3'] },
];

const LAYOUT_MARGIN = { l: 60, r: 10, t: 80, b: 50 };
// Every panel keeps the height the single analog panel used to have, so the
// canvas grows by one panel-height per added analog panel (analog area is now
// 3x, digital unchanged).
const PANEL_PX = 235;
const PANEL_GAP_PX = 22;

const BG_COLOR = '#0d1117';
const AXIS_COLOR = '#8b949e';
const GRID_COLOR = '#21262d';
const ZERO_COLOR = '#30363d';
const BOX_COLOR = '#30363d';

// ── Time view helpers ─────────────────────────────────
const VIEW_OPTIONS = {
    '1ms':  { label: '±1 ms',  range: 1 },
    '5ms':  { label: '±5 ms',  range: 5 },
    '10ms': { label: '±10 ms', range: 10 },
    '50ms': { label: '±50 ms', range: 50 },
};

// ── Channel lookup ────────────────────────────────────
// Waveform keys are raw column names minus their _v / _d suffix, but tolerate
// either form (and case differences) so a schema change does not blank a panel.
function resolveChannel(source, name) {
    if (!source) return null;
    if (name in source) return source[name];
    if ((name + '_v') in source) return source[name + '_v'];
    if ((name + '_d') in source) return source[name + '_d'];
    const lower = name.toLowerCase();
    for (const key of Object.keys(source)) {
        if (key.replace(/_[vd]$/, '').toLowerCase() === lower) return source[key];
    }
    return null;
}

// Reorder digital entries so DIGITAL_BOTTOM_ORDER lands last, in that order.
// Everything else keeps its incoming (alphabetical) order and stays above.
function orderDigitalEntries(entries) {
    const byName = new Map(entries);
    const pinned = DIGITAL_BOTTOM_ORDER.filter(name => byName.has(name));
    const pinnedSet = new Set(pinned);
    const rest = entries.filter(([name]) => !pinnedSet.has(name));
    return rest.concat(pinned.map(name => [name, byName.get(name)]));
}

// ── Panel collection & geometry ───────────────────────
// Panels that actually have data, in display order (top to bottom).
function collectPanels(analogSource, digitalSource) {
    const slots = [];
    for (const panel of ANALOG_PANELS) {
        const channels = panel.channels
            .map(name => ({ name, data: resolveChannel(analogSource, name) }))
            .filter(ch => ch.data && ch.data.time && ch.data.value);
        if (channels.length) slots.push({ kind: 'analog', title: panel.title, channels });
    }
    if (digitalSource && Object.keys(digitalSource).length > 0) {
        slots.push({ kind: 'digital', title: 'Interlock' });
    }
    return slots;
}

// Assign each slot a pixel height, a Y domain and its axis ids.
function assignSlotGeometry(slots) {
    const count = slots.length || 1;
    const plotArea = count * PANEL_PX + (count - 1) * PANEL_GAP_PX;
    const height = plotArea + LAYOUT_MARGIN.t + LAYOUT_MARGIN.b;
    let cursor = plotArea;  // px above the bottom of the plot area
    let analogOrdinal = 0;
    slots.forEach((slot, i) => {
        const bottom = cursor - PANEL_PX;
        slot.domain = [bottom / plotArea, cursor / plotArea];
        slot.yId = i === 0 ? 'y' : 'y' + (i + 1);
        slot.xId = i === 0 ? 'x' : 'x' + (i + 1);
        slot.isBottom = i === slots.length - 1;
        slot.isTop = i === 0;
        // Each analog panel gets its own legend: legend, legend2, legend3...
        if (slot.kind === 'analog') {
            slot.legendId = analogOrdinal === 0 ? 'legend' : 'legend' + (analogOrdinal + 1);
            analogOrdinal++;
        }
        cursor = bottom - PANEL_GAP_PX;
    });
    return { plotArea, height };
}

// ── Digital legend HTML builder ───────────────────────
function buildDigitalLegend(channelNames, containerId, slot, plotArea) {
    const existing = document.getElementById('digital-legend-box');
    if (existing) existing.remove();

    if (!channelNames || channelNames.length === 0 || !slot) return;

    const plotEl = document.getElementById(containerId);
    if (!plotEl) return;

    const wrapper = document.getElementById('waveform-plot-wrapper') || plotEl.parentElement;

    const box = document.createElement('div');
    box.id = 'digital-legend-box';
    box.className = 'digital-legend-box';

    let html = '';
    channelNames.forEach((name, i) => {
        const color = DIGITAL_COLORS[i % DIGITAL_COLORS.length];
        html += `<div class="digital-legend-item" style="color:${color};">${name}</div>`;
    });
    box.innerHTML = html;

    // Pin the legend inside the digital panel, whatever the stack height is.
    // Measured against the wrapper, since the panel sits inside its padding.
    const wrapperRect = wrapper.getBoundingClientRect();
    const plotRect = plotEl.getBoundingClientRect();
    const panelBottom = (wrapperRect.bottom - plotRect.bottom) + slot.domain[0] * plotArea;
    const panelHeight = (slot.domain[1] - slot.domain[0]) * plotArea;
    box.style.left = Math.round(plotRect.left - wrapperRect.left + LAYOUT_MARGIN.l + 8) + 'px';
    box.style.bottom = Math.round(panelBottom + 54) + 'px';
    box.style.maxHeight = Math.round(Math.max(70, panelHeight - 10)) + 'px';

    wrapper.appendChild(box);
}

// ── Main entry ────────────────────────────────────────
let analogStyles = {};  // per-signal {name: {color, linestyle, linewidth}} from server config

async function renderWaveform(containerId, eventData, analogMetrics, digitalPattern, styles) {
    analogStyles = (styles && typeof styles === 'object') ? styles : {};
    currentFilename = eventData.filename || '';
    const plotDiv = document.getElementById(containerId);
    if (!plotDiv) return;

    // Loading state
    plotDiv.innerHTML = '<div class="text-center text-secondary py-5"><div class="spinner-border spinner-border-sm me-2" role="status"></div>Loading waveforms...</div>';

    // Fetch real waveform data
    try {
        const resp = await fetch(`/api/events/${eventData.id}/waveforms`);
        if (!resp.ok) {
            renderSynthetic(containerId, analogMetrics, digitalPattern);
            return;
        }
        waveformData = await resp.json();
    } catch (e) {
        renderSynthetic(containerId, analogMetrics, digitalPattern);
        return;
    }

    if (!waveformData || !waveformData.analog || Object.keys(waveformData.analog).length === 0) {
        renderSynthetic(containerId, analogMetrics, digitalPattern);
        return;
    }

    renderRealData(containerId);
}

// ── Trace building (shared by real and synthetic data) ─
// `slots` must already carry axis ids from assignSlotGeometry, and `mask` is a
// boolean per sample (null = use every sample).
function buildChartTraces(slots, digitalSource, mask) {
    const traces = [];
    const keep = (arr) => (mask ? arr.filter((_, i) => mask[i]) : arr);

    // ── Analog traces, one group per panel ──
    for (const slot of slots) {
        if (slot.kind !== 'analog') continue;
        for (const ch of slot.channels) {
            const ls = analogLineStyle(ch.name, analogColorIndex(ch.name));
            traces.push({
                x: keep(ch.data.time),
                y: keep(ch.data.value),
                type: 'scatter',
                mode: 'lines',
                name: ch.name,
                // Each panel lists its own channels; legendgroup keeps a click
                // on BeamCurrent toggling it in all three panels at once.
                legendgroup: ch.name,
                legend: slot.legendId,
                showlegend: true,
                line: { color: ls.color, width: ls.width, dash: ls.dash },
                hovertemplate: `${slot.title} · ${ch.name}<br>t=%{x:.4f} ms<br>V=%{y:.4f}<extra></extra>`,
                xaxis: slot.xId,
                yaxis: slot.yId,
            });
        }
    }

    // ── Digital traces, offset-stacked in the bottom panel ──
    const digitalTraceNames = [];
    let digitalYRange = [-0.5, 2];
    const digitalSlot = slots.find(s => s.kind === 'digital');
    if (digitalSlot) {
        const entries = orderDigitalEntries(Object.entries(digitalSource)).slice(0, MAX_DIGITAL_CHANNELS);
        const count = entries.length;
        let maxOffset = 0;
        entries.forEach(([name, chData], idx) => {
            const offset = (count - 1 - idx) * DIGITAL_CHANNEL_GAP;
            traces.push({
                x: keep(chData.time),
                y: keep(chData.value).map(v => v + offset),
                type: 'scatter',
                mode: 'lines',
                name: name,
                showlegend: false,
                line: { color: DIGITAL_COLORS[idx % DIGITAL_COLORS.length], width: DIGITAL_LINE_WIDTH, shape: 'hv' },
                hovertemplate: `${name}<br>t=%{x:.4f} ms<br>%{y - offset}<extra></extra>`,
                xaxis: digitalSlot.xId,
                yaxis: digitalSlot.yId,
            });
            digitalTraceNames.push(name);
            maxOffset = Math.max(maxOffset, offset + 1);
        });
        digitalYRange = [-0.5, maxOffset + 1];
    }

    return { traces, digitalTraceNames, digitalYRange };
}

// ── Layout: stacked panels over one shared X axis ─────
function buildStackedLayout(slots, height, titleText, digitalYRange) {
    const layout = {
        height: height,
        title: {
            text: titleText,
            font: { color: '#e6edf3', size: 13 },
            y: 0.995,
        },
        paper_bgcolor: BG_COLOR,
        plot_bgcolor: BG_COLOR,
        font: { color: AXIS_COLOR, size: 11 },
        hovermode: 'closest',
        dragmode: 'pan',
        modebar: { bgcolor: '#161b22', color: AXIS_COLOR, activecolor: '#58a6ff' },
        margin: { ...LAYOUT_MARGIN },
    };

    slots.forEach((slot, i) => {
        const xKey = i === 0 ? 'xaxis' : 'xaxis' + (i + 1);
        const yKey = i === 0 ? 'yaxis' : 'yaxis' + (i + 1);
        const isDigital = slot.kind === 'digital';

        if (!isDigital) {
            // One legend per analog panel, pinned just inside the top-right of
            // that panel's box. Domains are paper coordinates, so the panel's
            // own domain doubles as the legend's anchor.
            layout[slot.legendId] = {
                x: 0.995,
                y: slot.domain[1] - 0.006,
                xanchor: 'right',
                yanchor: 'top',
                orientation: 'v',
                font: { size: 9, color: AXIS_COLOR },
                bgcolor: 'rgba(13,17,23,0.75)',
                bordercolor: BOX_COLOR,
                borderwidth: 1,
                tracegroupgap: 1,
                itemwidth: 30,
            };
        }

        layout[yKey] = {
            domain: slot.domain,
            anchor: slot.xId,
            title: { text: isDigital ? slot.title : `${slot.title} [a.u]`, font: { size: 10 } },
            gridcolor: GRID_COLOR,
            zerolinecolor: ZERO_COLOR,
            color: AXIS_COLOR,
            automargin: true,
            // Analog panels auto-scale in Y; the digital panel has synthetic
            // offsets, so its range stays pinned to the channel stack.
            fixedrange: isDigital,
            ...(isDigital
                ? { range: digitalYRange, showticklabels: false }
                : {
                    // Border box around each analog panel.
                    showline: true,
                    mirror: true,
                    linecolor: BOX_COLOR,
                    linewidth: 1,
                }),
        };

        layout[xKey] = {
            anchor: slot.yId,
            gridcolor: GRID_COLOR,
            zerolinecolor: ZERO_COLOR,
            color: AXIS_COLOR,
            automargin: true,
            rangeslider: { visible: false },
            showticklabels: slot.isBottom,
            title: slot.isBottom ? { text: 'Time (ms)' } : { text: '' },
            // The top panel owns the shared X range; every panel below follows it.
            ...(slot.isTop ? {} : { matches: 'x' }),
            ...(isDigital
                ? {}
                : {
                    // Border box around each analog panel.
                    showline: true,
                    mirror: true,
                    linecolor: BOX_COLOR,
                    linewidth: 1,
                }),
        };
    });

    return layout;
}

const PLOTLY_CONFIG = {
    responsive: true,
    displayModeBar: true,
    modeBarButtonsToRemove: ['sendDataToCloud', 'lasso2d', 'select2d', 'autoScale2d'],
    scrollZoom: true,
    displaylogo: false,
};

function drawChart(containerId, analogSource, digitalSource, mask, titleText) {
    // Geometry first: the traces need the axis ids it assigns.
    const slots = collectPanels(analogSource, digitalSource);
    const { plotArea, height } = assignSlotGeometry(slots);
    const { traces, digitalTraceNames, digitalYRange } = buildChartTraces(slots, digitalSource, mask);
    const layout = buildStackedLayout(slots, height, titleText, digitalYRange);

    // The container must grow with the stack, otherwise Plotly's responsive
    // resize squashes the chart back to the template's fixed height.
    const plotDiv = document.getElementById(containerId);
    if (plotDiv) {
        // Plotly appends to the container without clearing it, so the loading
        // placeholder would otherwise linger below the chart.
        plotDiv.innerHTML = '';
        plotDiv.style.height = height + 'px';
    }

    const digitalSlot = slots.find(s => s.kind === 'digital');
    return Plotly.newPlot(containerId, traces, layout, PLOTLY_CONFIG).then(() => {
        buildDigitalLegend(digitalTraceNames, containerId, digitalSlot, plotArea);
    });
}

// ── Render real data ──────────────────────────────────
function renderRealData(containerId) {
    const timeMs = waveformData.time_ms;
    const range = VIEW_OPTIONS[currentView].range;
    const mask = timeMs.map(t => t >= -range && t <= range);
    const viewTitle = VIEW_OPTIONS[currentView].label;
    const title = `Waveform — ${viewTitle}${currentFilename ? ' (' + currentFilename + ')' : ''}`;

    return drawChart(containerId, waveformData.analog, waveformData.digital, mask, title);
}

// ── Fallback synthetic Gaussian ───────────────────────
function renderSynthetic(containerId, analogMetrics, digitalPattern) {
    const range = VIEW_OPTIONS[currentView].range;
    const sampleRate = 1000;
    const n = Math.floor(range * sampleRate);
    const t = Array.from({ length: n }, (_, i) => (i / sampleRate) - range / 2);

    // Normalise the flat metrics into {time, value} so both paths share the
    // same panel and trace building code.
    const analogSource = {};
    for (const [name, m] of Object.entries(analogMetrics || {})) {
        const peak = (m && m.peak) || 0;
        const sigma = ((m && m.width) || 0.5) / 2.355;
        analogSource[name] = {
            time: t,
            value: t.map(ti => peak * Math.exp(-0.5 * (ti / sigma) ** 2)),
        };
    }

    const digitalRate = 200;
    const dn = Math.floor(range * digitalRate);
    const dt = Array.from({ length: dn }, (_, i) => (i / digitalRate) - range / 2);
    const digitalSource = {};
    for (const [name, val] of Object.entries(digitalPattern || {})) {
        digitalSource[name] = { time: dt, value: dt.map(() => (val ? 1 : 0)) };
    }

    syntheticAnalog = analogSource;
    syntheticDigital = digitalSource;

    const title = `Waveform — Synthetic (no data)${currentFilename ? ' (' + currentFilename + ')' : ''}`;
    return drawChart(containerId, analogSource, digitalSource, null, title);
}

// ── Set view range ────────────────────────────────────
function setView(viewKey) {
    currentView = viewKey;
    const plotDiv = document.getElementById('waveform-plot');
    if (!plotDiv) return;
    if (waveformData) {
        renderRealData('waveform-plot');
    } else if (syntheticAnalog) {
        // Re-render from the cached sources; the caller no longer has to pass
        // the metrics back in.
        const title = `Waveform — Synthetic (no data)${currentFilename ? ' (' + currentFilename + ')' : ''}`;
        drawChart('waveform-plot', syntheticAnalog, syntheticDigital, null, title);
    }
}