/* Waveform Viewer — newest-first, left/right navigation, read-only info panel. */
(function () {
    'use strict';

    const page = document.getElementById('wfg-page');
    if (!page) return;

    const SCOPES = ['forward', 'cavity', 'reflect'];
    const state = {
        events: [],
        years: [],
        yearCounts: {},
        year: '',
        index: 0,
        detailCache: new Map(),
        requestSeq: 0,
        touch: null,
    };

    const $ = (id) => document.getElementById(id);

    function setStatus(text) {
        const el = $('wfg-status');
        if (el) el.textContent = text || '';
    }

    function formatTime(ts) {
        if (!ts) return '';
        return String(ts).replace('T', ' ').slice(0, 19);
    }

    function caseClass(caseId) {
        const id = Number.isInteger(caseId) && caseId >= 0 && caseId <= 21 ? caseId : 0;
        return 'case-badge-' + id;
    }

    // ── List loading ────────────────────────────────────────
    async function loadList(year) {
        setStatus('목록을 불러오는 중…');
        const query = year ? '?year=' + encodeURIComponent(year) : '';
        const response = await fetch('/api/waveforms' + query, { cache: 'no-store' });
        if (!response.ok) throw new Error('HTTP ' + response.status);
        const data = await response.json();
        state.events = Array.isArray(data.events) ? data.events : [];
        state.years = Array.isArray(data.years) ? data.years : [];
        state.yearCounts = data.year_counts || {};
        state.year = year ? String(year) : '';
        fillYearSelect();
    }

    function fillYearSelect() {
        const select = $('wfg-year');
        if (!select) return;
        select.innerHTML = '';

        const all = document.createElement('option');
        all.value = '';
        all.textContent = '전체 연도';
        select.appendChild(all);

        state.years.forEach((year) => {
            const option = document.createElement('option');
            option.value = year;
            option.textContent = year + '년 (' + (state.yearCounts[year] || 0) + ')';
            select.appendChild(option);
        });

        select.value = state.year;
    }

    // ── Rendering ───────────────────────────────────────────
    function setImage(scope, url) {
        const panel = document.querySelector('.wfg-panel[data-scope="' + scope + '"]');
        const img = $('wfg-img-' + scope);
        if (!panel || !img) return;

        if (!url) {
            img.removeAttribute('src');
            panel.classList.add('is-empty');
            return;
        }

        panel.classList.remove('is-empty');
        img.onerror = function () {
            img.removeAttribute('src');
            panel.classList.add('is-empty');
        };
        if (img.getAttribute('src') !== url) {
            img.src = url;
        }
    }

    function clearImages() {
        SCOPES.forEach((scope) => setImage(scope, ''));
    }

    function show(index, options) {
        options = options || {};
        if (!state.events.length) {
            clearImages();
            $('wfg-counter').textContent = '0 / 0';
            $('wfg-event-id').textContent = '—';
            $('wfg-time').textContent = '';
            $('wfg-detail-link').href = '#';
            $('wfg-fault').textContent = '—';
            $('wfg-fault').className = 'badge fs-6 case-badge-0';
            $('wfg-fault-note').classList.add('d-none');
            $('wfg-conf').textContent = '';
            $('wfg-beamtime').textContent = '—';
            $('wfg-notes').textContent = '—';
            $('wfg-diagnosis').textContent = '—';
            $('wfg-sequence').textContent = '';
            $('wfg-prev').disabled = true;
            $('wfg-next').disabled = true;
            $('wfg-latest').classList.add('d-none');
            setStatus('이미지가 있는 이벤트가 없습니다.');
            return;
        }

        index = Math.max(0, Math.min(state.events.length - 1, index));
        state.index = index;
        const event = state.events[index];

        $('wfg-counter').textContent = (index + 1) + ' / ' + state.events.length;
        $('wfg-event-id').textContent = event.id;
        $('wfg-time').textContent = formatTime(event.timestamp);
        $('wfg-detail-link').href = '/events/' + encodeURIComponent(event.id);
        $('wfg-latest').classList.toggle('d-none', index !== 0);

        const fault = event.effective_fault || 'Unknown';
        const badge = $('wfg-fault');
        badge.textContent = fault;
        badge.className = 'badge fs-6 ' + caseClass(event.case_id);

        const manual = !!(event.user_fault_type || '').trim()
            && event.user_fault_type !== event.fault_type;
        $('wfg-fault-note').classList.toggle('d-none', !manual);

        const confidence = Number(event.fault_confidence || 0);
        $('wfg-conf').textContent = confidence > 0
            ? Math.round(confidence * 100) + '% confidence'
            : '';

        $('wfg-beamtime').textContent = event.user_beam_time || '—';

        SCOPES.forEach((scope) => {
            setImage(scope, (event.images || {})[scope]);
        });

        $('wfg-prev').disabled = index <= 0;
        $('wfg-next').disabled = index >= state.events.length - 1;

        if (options.updateUrl !== false) updateUrl(event);
        renderDetail(event.id);
        preload(index + 1);
        preload(index - 1);
    }

    function updateUrl(event) {
        const params = new URLSearchParams();
        if (state.year) params.set('year', state.year);
        params.set('event', event.id);
        const next = location.pathname + '?' + params.toString();
        if (location.pathname + location.search !== next) {
            history.replaceState(null, '', next);
        }
    }

    // ── Detail panel ────────────────────────────────────────
    async function renderDetail(eventId) {
        const seq = ++state.requestSeq;
        let detail = state.detailCache.get(eventId);

        if (!detail) {
            try {
                const response = await fetch(
                    '/api/waveforms/' + encodeURIComponent(eventId),
                    { cache: 'no-store' }
                );
                if (!response.ok) throw new Error('HTTP ' + response.status);
                detail = await response.json();
                state.detailCache.set(eventId, detail);
            } catch (err) {
                if (seq === state.requestSeq) {
                    setStatus('상세 정보를 불러오지 못했습니다.');
                }
                return;
            }
        }

        if (seq !== state.requestSeq) return;

        renderSequence(detail.time_groups);
        $('wfg-notes').textContent = detail.notes || '—';
        const diagnosis = [detail.case_description, detail.case_fault]
            .filter(Boolean)
            .join(' — ');
        $('wfg-diagnosis').textContent = diagnosis || '—';
        setStatus('');
    }

    function renderSequence(timeGroups) {
        const host = $('wfg-sequence');
        host.textContent = '';
        if (!timeGroups) {
            host.textContent = 'No sequence data';
            return;
        }

        const addGroup = (label, color, group, limit) => {
            if (!group || !Array.isArray(group.events) || !group.events.length) return;

            const title = document.createElement('div');
            title.className = 'wfg-seq-title';
            title.style.color = color;
            title.textContent = label;
            host.appendChild(title);

            const list = document.createElement('ul');
            list.className = 'wfg-seq-list';

            group.events.slice(0, limit).forEach((item) => {
                const li = document.createElement('li');

                const channel = document.createElement('span');
                channel.className = 'text-light';
                channel.textContent = String(item.channel || '')
                    .replace('_v', '')
                    .replace('_d', '');

                const when = document.createElement('span');
                when.className = 'text-secondary ms-1';
                when.textContent = (Number(item.time_s || 0) * 1000).toFixed(2) + ' ms';

                const value = document.createElement('span');
                value.className = 'ms-1';
                value.textContent = '[' + Number(item.value || 0).toFixed(3) + ']';

                li.appendChild(channel);
                li.appendChild(when);
                li.appendChild(value);
                list.appendChild(li);
            });

            if (group.events.length > limit) {
                const more = document.createElement('li');
                more.className = 'text-secondary';
                more.textContent = '+' + (group.events.length - limit) + ' more';
                list.appendChild(more);
            }

            host.appendChild(list);
        };

        const hasFirst = timeGroups.first && Array.isArray(timeGroups.first.events) && timeGroups.first.events.length;
        const hasSecond = timeGroups.second && Array.isArray(timeGroups.second.events) && timeGroups.second.events.length;
        if (!hasFirst && !hasSecond) {
            host.textContent = 'No sequence data';
            return;
        }

        addGroup('① First', '#58a6ff', timeGroups.first, 5);
        addGroup('② Second (fastest 2)', '#d29922', timeGroups.second, 2);
    }

    // ── Navigation ──────────────────────────────────────────
    function go(delta) {
        if (!state.events.length) return;
        show(state.index + delta);
    }

    function preload(index) {
        const event = state.events[index];
        if (!event || !event.images) return;
        Object.values(event.images).forEach((url) => {
            const img = new Image();
            img.src = url;
        });
    }

    $('wfg-prev').addEventListener('click', () => go(-1));
    $('wfg-next').addEventListener('click', () => go(1));

    document.addEventListener('keydown', (event) => {
        const tag = event.target && event.target.tagName;
        if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return;

        if (event.key === 'ArrowLeft') {
            event.preventDefault();
            go(-1);
        } else if (event.key === 'ArrowRight') {
            event.preventDefault();
            go(1);
        } else if (event.key === 'Home') {
            event.preventDefault();
            show(0);
        } else if (event.key === 'End') {
            event.preventDefault();
            show(state.events.length - 1);
        }
    });

    page.addEventListener('touchstart', (event) => {
        if (event.touches.length !== 1) return;
        state.touch = {
            x: event.touches[0].clientX,
            y: event.touches[0].clientY,
        };
    }, { passive: true });

    page.addEventListener('touchend', (event) => {
        if (!state.touch) return;
        const touch = event.changedTouches[0];
        const dx = touch.clientX - state.touch.x;
        const dy = touch.clientY - state.touch.y;
        state.touch = null;

        // Horizontal swipe only: never fight vertical page scrolling.
        if (Math.abs(dx) > 60 && Math.abs(dx) > Math.abs(dy) * 2) {
            go(dx < 0 ? 1 : -1);
        }
    }, { passive: true });

    $('wfg-year').addEventListener('change', async (event) => {
        const year = event.target.value;
        try {
            await loadList(year);
            show(0);
        } catch (err) {
            setStatus('목록을 불러오지 못했습니다.');
        }
    });

    // Keep the details block open on desktop, collapsed on phones.
    function syncDetails() {
        const details = $('wfg-details');
        if (!details) return;
        if (window.innerWidth > 900) {
            details.setAttribute('open', '');
        } else {
            details.removeAttribute('open');
        }
    }
    window.addEventListener('resize', syncDetails);

    // ── Start ───────────────────────────────────────────────
    const params = new URLSearchParams(location.search);
    const initialEvent = page.dataset.initialEvent || params.get('event') || '';
    const initialYear = page.dataset.initialYear || params.get('year') || '';
    // An explicit event may belong to another year: start with the full list.
    const yearFilter = initialEvent ? '' : initialYear;

    (async function init() {
        syncDetails();
        try {
            await loadList(yearFilter);
            let index = 0;
            if (initialEvent) {
                const found = state.events.findIndex((item) => item.id === initialEvent);
                if (found >= 0) index = found;
            }
            show(index, { updateUrl: false });
        } catch (err) {
            clearImages();
            setStatus('목록을 불러오지 못했습니다.');
        }
    })();
})();
