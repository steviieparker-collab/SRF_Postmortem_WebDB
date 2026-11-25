/* SRF Postmortem RAG chat UI */
(function () {
    const log = document.getElementById('chat-log');
    const input = document.getElementById('chat-input');
    const sendBtn = document.getElementById('chat-send');
    const meta = document.getElementById('chat-meta');
    const sourcesEl = document.getElementById('sources');
    const suggestionsEl = document.getElementById('suggestions');
    const indexBadge = document.getElementById('index-badge');

    function esc(value) {
        return String(value ?? '').replace(/[&<>"']/g, function (c) {
            return {'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[c];
        });
    }

    function renderAnswer(text) {
        let h = esc(text);
        h = h.replace(/^## (.+)$/gm, '<h6 class="text-info mt-3 mb-1">$1</h6>');
        h = h.replace(/\[E:([0-9_]+)\]/g,
            '<a class="badge bg-primary text-decoration-none me-1" href="/events/$1">E:$1</a>');
        h = h.replace(/\[A:([^\/\]]+)\/([^\]]+)\]/g,
            '<a class="badge bg-danger text-decoration-none me-1" href="/events/$1">A:$2</a>');
        h = h.replace(/\[PVS:([^\/\]]+)\/([^\]]+)\]/g,
            '<a class="badge bg-warning text-dark text-decoration-none me-1" href="/events/$1">PVS:$2</a>');
        h = h.replace(/\[PV:([^\]]+)\]/g,
            '<a class="badge bg-success text-decoration-none me-1" href="/alarm-dashboard">PV:$1</a>');
        h = h.replace(/\[FT:([^\]]+)\]/g,
            '<a class="badge bg-secondary text-decoration-none me-1" href="/stats">FT:$1</a>');
        h = h.replace(/\n/g, '<br>');
        return h;
    }

    function addMessage(role, text) {
        const div = document.createElement('div');
        div.className = 'chat-msg chat-' + role;
        div.innerHTML = '<div class="small text-secondary mb-1">' + esc(role) + '</div>' +
            (role === 'assistant' ? renderAnswer(text) : esc(text).replace(/\n/g, '<br>'));
        log.appendChild(div);
        log.scrollTop = log.scrollHeight;
        return div;
    }

    function sourceBadge(type) {
        const map = {
            event: 'bg-primary', alarm: 'bg-danger', pv_selection: 'bg-warning text-dark',
            pv: 'bg-success', fault: 'bg-secondary', stats: 'bg-info text-dark',
            alarm_status: 'bg-info text-dark'
        };
        return '<span class="badge ' + (map[type] || 'bg-secondary') + ' me-1">' + esc(type) + '</span>';
    }

    function renderSources(sources) {
        if (!sources || !sources.length) {
            sourcesEl.innerHTML = '<div class="small text-secondary p-2">출처가 없습니다.</div>';
            return;
        }
        sourcesEl.innerHTML = sources.map(function (s) {
            const href = s.url || '#';
            return '<div class="source-item small">' + sourceBadge(s.type) +
                '<a href="' + esc(href) + '" class="text-decoration-none">' + esc(s.title || s.id) + '</a></div>';
        }).join('');
    }

    async function send() {
        const message = input.value.trim();
        if (!message) return;
        addMessage('user', message);
        input.value = '';
        sendBtn.disabled = true;
        meta.textContent = '검색 및 생성 중…';
        const placeholder = addMessage('assistant', '…');

        try {
            const res = await fetch('/api/chat', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({message: message})
            });
            const data = await res.json();
            if (!res.ok) throw new Error(data.answer || ('HTTP ' + res.status));
            placeholder.innerHTML = '<div class="small text-secondary mb-1">assistant</div>' +
                renderAnswer(data.answer || '');
            renderSources(data.sources || []);
            const bits = [];
            if (data.intent) bits.push('intent=' + data.intent);
            if (data.model) bits.push(data.model);
            if (data.elapsed_ms != null) bits.push(data.elapsed_ms + 'ms');
            if (data.llm_ok === false) bits.push('LLM fallback');
            meta.textContent = bits.join(' · ');
        } catch (e) {
            placeholder.innerHTML = '<div class="small text-danger">오류</div>' + esc(String(e));
            meta.textContent = '요청 실패';
        } finally {
            sendBtn.disabled = false;
            log.scrollTop = log.scrollHeight;
        }
    }

    async function loadSuggestions() {
        try {
            const res = await fetch('/api/chat/suggestions');
            const data = await res.json();
            const items = data.suggestions || [];
            suggestionsEl.innerHTML = items.map(function (q) {
                return '<button type="button" class="btn btn-sm btn-outline-secondary w-100 text-start mb-1 suggestion">' +
                    esc(q) + '</button>';
            }).join('');
            suggestionsEl.querySelectorAll('.suggestion').forEach(function (btn) {
                btn.addEventListener('click', function () {
                    input.value = btn.textContent;
                    send();
                });
            });
        } catch (e) {
            suggestionsEl.innerHTML = '<div class="small text-secondary p-2">예시를 불러오지 못했습니다.</div>';
        }
    }

    function loadStatus() {
        const s = window.RAG_STATUS || {};
        if (s.available) {
            indexBadge.textContent = (s.documents || 0) + ' docs / ' + (s.chunks || 0) + ' chunks';
            indexBadge.className = 'badge bg-success';
        } else {
            indexBadge.textContent = 'index not built';
            indexBadge.className = 'badge bg-danger';
        }
        if (s.api_key_configured === false) {
            meta.textContent = 'OPENROUTER_API_KEY가 설정되지 않았습니다. .env를 확인하세요.';
        }
    }

    sendBtn.addEventListener('click', send);
    input.addEventListener('keydown', function (e) {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            send();
        }
    });

    loadStatus();
    loadSuggestions();
})();
