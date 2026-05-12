/**
 * SRF Postmortem Viewer — Chart Rendering
 *
 * Fetches real waveform data from the parquet file via API
 * and renders interactive Plotly charts with analog and digital
 * channels in separate subplots (analog top, digital bottom).
 *
 * Digital channel names are rendered as a custom HTML legend box
 * to avoid Plotly legend line-height limitations.
 */

let currentView = '1ms';
let waveformData = null;
let currentFilename = '';

// ── Color palettes ────────────────────────────────────
const ANALOG_COLORS = [
    '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728',
    '#9467bd', '#8c564b', '#e377c2', '#7f7f7f',
    '#bcbd22', '#17becf',
];
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

// ── Time view helpers ─────────────────────────────────
const VIEW_OPTIONS = {
    '1ms':  { label: '±1 ms',  range: 1 },
    '5ms':  { label: '±5 ms',  range: 5 },
    '10ms': { label: '±10 ms', range: 10 },
    '50ms': { label: '±50 ms', range: 50 },
};

// ── Digital legend HTML builder ───────────────────────
function buildDigitalLegend(channelNames, containerId) {
    const existing = document.getElementById('digital-legend-box');
    if (existing) existing.remove();

    if (!channelNames || channelNames.length === 0) return;

    // Look for a position-relative wrapper; fall back to plot container's parent
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

    wrapper.appendChild(box);
}

// ── Main entry ────────────────────────────────────────
async function renderWaveform(containerId, eventData, analogMetrics, digitalPattern) {
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

// ── Subplot width & height helper ─────────────────────
function subplotDimensions(containerId) {
    const el = document.getElementById(containerId);
    const w = (el && el.offsetWidth) || 700;
    const h = 600; // Reduced height
    return { w, h };
}

// ── Render real data (shared X axis, separate Y axes) ─
function renderRealData(containerId) {
    const traces = [];

    // Time mask for view range
    const timeMs = waveformData.time_ms;
    const range = VIEW_OPTIONS[currentView].range;
    const mask = timeMs.map(t => t >= -range && t <= range);
    const viewTitle = VIEW_OPTIONS[currentView].label;

    // ── Analog traces (subplot row 1) ──
    let chIdx = 0;
    for (const [name, chData] of Object.entries(waveformData.analog)) {
        const filtered = {
            x: chData.time.filter((_, i) => mask[i]),
            y: chData.value.filter((_, i) => mask[i]),
        };
        // Keep Reflect_SRF at original amplitude (no scaling)
        traces.push({
            x: filtered.x,
            y: filtered.y,
            type: 'scatter',
            mode: 'lines',
            name: name,
            legendgroup: 'analog',
            line: { color: ANALOG_COLORS[chIdx % ANALOG_COLORS.length], width: 1.5 },
            hovertemplate: `${name}<br>t=%{x:.4f} ms<br>V=%{y:.4f}<extra></extra>`,
            xaxis: 'x',
            yaxis: 'y',
        });
        chIdx++;
    }

    // ── Digital traces (subplot row 2) ──
    let dIdx = 0;
    const digitalTraceNames = [];
    let maxDigitalOffset = 0;
    const digitalEntries = Object.entries(waveformData.digital);
    const numDigitalChannels = Math.min(digitalEntries.length, 25);
    for (const [name, chData] of digitalEntries) {
        if (dIdx >= 25) break;
        const filtered = {
            x: chData.time.filter((_, i) => mask[i]),
            y: chData.value.filter((_, i) => mask[i]),
        };
        const color = DIGITAL_COLORS[dIdx % DIGITAL_COLORS.length];
        const offset = (numDigitalChannels - 1 - dIdx) * 1.1;
        traces.push({
            x: filtered.x,
            y: filtered.y.map(v => v + offset),
            type: 'scatter',
            mode: 'lines',
            name: name,
            showlegend: false,
            line: { color, width: 1.2, shape: 'hv' },
            hovertemplate: `${name}<br>t=%{x:.4f} ms<br>%{y - offset}<extra></extra>`,
            xaxis: 'x2',
            yaxis: 'y2',
        });
        digitalTraceNames.push(name);
        maxDigitalOffset = Math.max(maxDigitalOffset, offset + 1);
        dIdx++;
    }

    const digitalPlotHeight = maxDigitalOffset + 1;
    const { w, h } = subplotDimensions(containerId);

    // ── Layout with subplots ─────────────────────────────
    const layout = {
        height: h,
        grid: {
            rows: 2,
            columns: 1,
            pattern: 'independent',
            subplots: [['xy'], ['x2y2']],
            roworder: 'top to bottom',
        },
        title: {
            text: `Waveform — ${viewTitle}${currentFilename ? ' (' + currentFilename + ')' : ''}`,
            font: { color: '#e6edf3', size: 13 },
            y: 0.99,
        },
        paper_bgcolor: '#0d1117',
        plot_bgcolor: '#0d1117',
        font: { color: '#8b949e', size: 11 },
        hovermode: 'closest',
        dragmode: 'pan',
        modebar: { bgcolor: '#161b22', color: '#8b949e', activecolor: '#58a6ff' },
        legend: {
            x: 1.0, y: 1,
            xanchor: 'right',
            yanchor: 'top',
            font: { size: 8, color: '#8b949e' },
            bgcolor: 'rgba(0,0,0,0)',
        },
        margin: { l: 60, r: 10, t: 80, b: 50 },

        yaxis: {
            title: 'Analog [a.u]',
            domain: [0.5, 1],
            gridcolor: '#21262d',
            zerolinecolor: '#30363d',
            color: '#8b949e',
            range: [0, 2],
            fixedrange: true,
        },
        yaxis2: {
            title: 'Interlock',
            domain: [0, 0.5],
            range: [-0.5, digitalPlotHeight + 1],
            gridcolor: '#21262d',
            zerolinecolor: '#30363d',
            color: '#8b949e',
            automargin: true,
            fixedrange: true,
            showticklabels: false,
        },
        xaxis: {
            title: '',
            gridcolor: '#21262d',
            zerolinecolor: '#30363d',
            color: '#8b949e',
            automargin: true,
            ticklabelposition: 'inside top',
            rangeslider: { visible: false },
        },
        xaxis2: {
            title: 'Time (ms)',
            gridcolor: '#21262d',
            zerolinecolor: '#30363d',
            color: '#8b949e',
            automargin: true,
            matches: 'x',
            ticklabelposition: 'inside top',
            showticklabels: true,
        },
    };

    const config = {
        responsive: true,
        displayModeBar: true,
        modeBarButtonsToRemove: ['sendDataToCloud', 'lasso2d', 'select2d', 'autoScale2d'],
        scrollZoom: true,
        displaylogo: false,
    };

    Plotly.newPlot(containerId, traces, layout, config).then(() => {
        // Build digital legend box after plot renders
        buildDigitalLegend(digitalTraceNames, containerId);
    });
}

// ── Fallback synthetic Gaussian ───────────────────────
function renderSynthetic(containerId, analogMetrics, digitalPattern) {
    const traces = [];
    const range = VIEW_OPTIONS[currentView].range;
    const timeWindow = range;

    let chIdx = 0;
    if (analogMetrics && Object.keys(analogMetrics).length > 0) {
        for (const [ch, chData] of Object.entries(analogMetrics)) {
            const peak = chData.peak || 0;
            const width = chData.width || 0.5;
            const sampleRate = 1000;
            const n = Math.floor(timeWindow * sampleRate);
            const t = Array.from({ length: n }, (_, i) => (i / sampleRate) - timeWindow / 2);
            const sigma = width / 2.355;
            const y = t.map(ti => peak * Math.exp(-0.5 * (ti / sigma) ** 2));

            traces.push({
                x: t, y,
                type: 'scatter', mode: 'lines',
                name: ch,
                legendgroup: 'analog',
                line: { color: ANALOG_COLORS[chIdx % ANALOG_COLORS.length], width: 1.5 },
                hovertemplate: `${ch}<br>t=%{x:.4f}ms<br>V=%{y:.4f}<extra></extra>`,
                xaxis: 'x',
                yaxis: 'y',
            });
            chIdx++;
        }
    } else {
        traces.push({ x: [0], y: [0], type: 'scatter', mode: 'markers', name: 'No data', marker: { size: 1, opacity: 0 }, xaxis: 'x', yaxis: 'y' });
    }

    const digitalTraceNames = [];
    let maxSyntheticDigitalOffset = 0;
    if (digitalPattern && Object.keys(digitalPattern).length > 0) {
        let dIdx = 0;
        maxSyntheticDigitalOffset = 0;
        const digitalEntries = Object.entries(digitalPattern);
        const numDigitalChannels = Math.min(digitalEntries.length, 8);
        for (const [ch, val] of digitalEntries) {
            if (dIdx >= 8) break;
            const sampleRate = 200;
            const n = Math.floor(timeWindow * sampleRate);
            const t = Array.from({ length: n }, (_, i) => (i / sampleRate) - timeWindow / 2);
            const offset = (numDigitalChannels - 1 - dIdx) * 1.1;
            const y = t.map(() => (val ? 1 : 0) + offset);
            traces.push({
                x: t, y,
                type: 'scatter', mode: 'lines',
                name: ch,
                showlegend: false,
                line: { color: DIGITAL_COLORS[dIdx % DIGITAL_COLORS.length], width: 1.2, shape: 'hv' },
                hovertemplate: `${ch}<br>t=%{x:.4f}ms<br>%{y - offset}<extra></extra>`,
                xaxis: 'x2',
                yaxis: 'y2',
            });
            digitalTraceNames.push(ch);
            maxSyntheticDigitalOffset = Math.max(maxSyntheticDigitalOffset, offset + 1);
            dIdx++;
        }
    }

    const { w, h } = subplotDimensions(containerId);

    const synthLayout = {
        height: h,
        grid: {
            rows: 2,
            columns: 1,
            pattern: 'independent',
            subplots: [['xy'], ['x2y2']],
            roworder: 'top to bottom',
        },
        title: {
            text: `Waveform — Synthetic (no data)${currentFilename ? ' (' + currentFilename + ')' : ''}`,
            font: { color: '#e6edf3', size: 8 },
            y: 0.99,
        },
        paper_bgcolor: '#0d1117',
        plot_bgcolor: '#0d1117',
        font: { color: '#8b949e', size: 11 },
        hovermode: 'closest',
        dragmode: 'pan',
        modebar: { bgcolor: '#161b22', color: '#8b949e', activecolor: '#58a6ff' },
        legend: {
            x: 1.0, y: 1,
            xanchor: 'right',
            yanchor: 'top',
            font: { size: 5, color: '#8b949e' },
            bgcolor: 'rgba(0,0,0,0)',
        },
        margin: { l: 60, r: 50, t: 80, b: 50 },
        yaxis: {
            title: 'Analog [a.u]',
            domain: [0.5, 1],
            gridcolor: '#21262d',
            zerolinecolor: '#30363d',
            color: '#8b949e',
            range: [0, 2],
            fixedrange: true,
        },
        yaxis2: {
            title: 'Interlock',
            domain: [0, 0.5],
            range: [-0.5, maxSyntheticDigitalOffset + 1],
            gridcolor: '#21262d',
            zerolinecolor: '#30363d',
            color: '#8b949e',
            automargin: true,
            fixedrange: true,
            showticklabels: false,
        },
        xaxis: {
            title: '',
            gridcolor: '#21262d',
            zerolinecolor: '#30363d',
            color: '#8b949e',
            automargin: true,
            ticklabelposition: 'inside top',
            rangeslider: { visible: false },
        },
        xaxis2: {
            title: 'Time (ms)',
            gridcolor: '#21262d',
            zerolinecolor: '#30363d',
            color: '#8b949e',
            automargin: true,
            matches: 'x',
            ticklabelposition: 'inside top',
            showticklabels: true,
        },
    };

    const config = {
        responsive: true, displayModeBar: true,
        modeBarButtonsToRemove: ['sendDataToCloud', 'lasso2d', 'select2d', 'autoScale2d'],
        scrollZoom: true, displaylogo: false,
    };

    Plotly.newPlot(containerId, traces, synthLayout, config).then(() => {
        buildDigitalLegend(digitalTraceNames, containerId);
    });
}

// ── Set view range ────────────────────────────────────
function setView(viewKey) {
    currentView = viewKey;
    const plotDiv = document.getElementById('waveform-plot');
    if (plotDiv) {
        if (waveformData) {
            renderRealData('waveform-plot');
        } else {
            renderSynthetic('waveform-plot', SRF_ANALOG_METRICS, SRF_DIGITAL_PATTERN);
        }
    }
}
