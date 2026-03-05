(function () {
    'use strict';

    const STORAGE_KEY = 'panopticum-data-diff';

    function getItems() {
        try {
            const raw = localStorage.getItem(STORAGE_KEY);
            const parsed = raw ? JSON.parse(raw) : {};
            const items = parsed.items;
            return Array.isArray(items) ? items : [];
        } catch (e) {
            return [];
        }
    }

    function setItems(items) {
        try {
            localStorage.setItem(STORAGE_KEY, JSON.stringify({ items: items }));
            window.dispatchEvent(new StorageEvent('storage', {
                key: STORAGE_KEY,
                newValue: JSON.stringify({ items: items })
            }));
        } catch (e) {
        }
    }

    function addItem(payload) {
        if (!payload || !payload.data) return;
        const items = getItems();
        const id = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
            const r = Math.random() * 16 | 0;
            const v = c === 'x' ? r : (r & 0x3 | 0x8);
            return v.toString(16);
        });
        const item = {
            id: id,
            source: payload.source || 'unknown',
            connectionId: payload.connectionId,
            connectionName: payload.connectionName || '',
            label: payload.label || '',
            data: payload.data,
            dataFormat: payload.dataFormat || 'json',
            addedAt: new Date().toISOString()
        };
        items.push(item);
        setItems(items);
    }

    function clearItems() {
        setItems([]);
    }

    function updateHeaderVisibility() {
        const link = document.getElementById('data-diff-header-link');
        if (!link) return;
        const items = getItems();
        link.style.display = items.length > 0 ? '' : 'none';
    }

    function escapeHtml(str) {
        if (str == null) return '';
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }

    function renderCard(item) {
        const card = document.createElement('div');
        card.className = 'data-diff-card';
        const title = document.createElement('div');
        title.className = 'data-diff-card-title';
        title.textContent = item.label || item.connectionName || 'Item';
        card.appendChild(title);
        const body = document.createElement('div');
        body.className = 'data-diff-card-body';
        if (item.dataFormat === 'keyValue') {
            let data;
            try {
                data = typeof item.data === 'string' ? JSON.parse(item.data) : item.data;
            } catch (e) {
                data = null;
            }
            if (data && Array.isArray(data)) {
                const table = document.createElement('table');
                table.className = 'query-table';
                const thead = document.createElement('thead');
                thead.innerHTML = '<tr><th class="query-th">Field</th><th class="query-th">Value</th></tr>';
                table.appendChild(thead);
                const tbody = document.createElement('tbody');
                data.forEach(function (row) {
                    const tr = document.createElement('tr');
                    tr.innerHTML = '<td class="query-cell-mono">' + escapeHtml(row.name || row.key || '') + '</td>' +
                        '<td class="query-cell-mono">' + escapeHtml(String(row.value != null ? row.value : '')) + '</td>';
                    tbody.appendChild(tr);
                });
                table.appendChild(tbody);
                body.appendChild(table);
            } else {
                const pre = document.createElement('pre');
                pre.className = 'detail-json';
                pre.textContent = typeof item.data === 'string' ? item.data : JSON.stringify(item.data, null, 2);
                body.appendChild(pre);
            }
        } else {
            const pre = document.createElement('pre');
            pre.className = 'detail-json';
            const code = document.createElement('code');
            code.className = 'language-json';
            code.textContent = typeof item.data === 'string' ? item.data : JSON.stringify(item.data, null, 2);
            pre.appendChild(code);
            body.appendChild(pre);
        }
        card.appendChild(body);
        return card;
    }

    function renderDiffPage() {
        const container = document.getElementById('data-diff-container');
        if (!container) return;
        const items = getItems();
        container.innerHTML = '';
        if (items.length === 0) {
            const empty = document.createElement('p');
            empty.className = 'text-dim';
            empty.setAttribute('data-diff-empty', '');
            empty.textContent = container.getAttribute('data-diff-empty-text') || 'No items to compare.';
            container.appendChild(empty);
            return;
        }
        const grid = document.createElement('div');
        grid.className = 'data-diff-grid';
        items.forEach(function (item) {
            grid.appendChild(renderCard(item));
        });
        container.appendChild(grid);
        container.querySelectorAll('code.language-json').forEach(function (el) {
            if (typeof Prism !== 'undefined' && Prism.highlightElement) {
                Prism.highlightElement(el);
            }
        });
    }

    function initAddButtons() {
        document.querySelectorAll('.data-diff-add').forEach(function (btn) {
            if (btn.dataset.diffInit) return;
            btn.dataset.diffInit = '1';
            btn.addEventListener('click', function () {
                const payloadStr = btn.getAttribute('data-diff-payload');
                if (!payloadStr) return;
                try {
                    const payload = JSON.parse(payloadStr);
                    addItem(payload);
                    const addedText = btn.getAttribute('data-diff-added-text') || 'Added';
                    const origText = btn.textContent;
                    btn.textContent = addedText;
                    btn.classList.add('added');
                    setTimeout(function () {
                        btn.textContent = origText;
                        btn.classList.remove('added');
                    }, 1500);
                } catch (e) {
                }
            });
        });
    }

    function init() {
        updateHeaderVisibility();
        initAddButtons();
        if (window.location.pathname === '/diff') {
            renderDiffPage();
            const clearBtn = document.getElementById('data-diff-clear-btn');
            if (clearBtn) {
                clearBtn.addEventListener('click', function () {
                    clearItems();
                    renderDiffPage();
                    updateHeaderVisibility();
                });
            }
        }
    }

    document.addEventListener('DOMContentLoaded', init);

    window.addEventListener('storage', function (ev) {
        if (ev.key === STORAGE_KEY) {
            updateHeaderVisibility();
        }
    });

    function addHtmxListener() {
        if (document.body) {
            document.body.addEventListener('htmx:afterSwap', function (ev) {
                const target = ev.detail && ev.detail.target;
                if (target && typeof target.querySelectorAll === 'function') {
                    updateHeaderVisibility();
                    initAddButtons();
                }
            });
        }
    }
    if (document.body) {
        addHtmxListener();
    } else {
        document.addEventListener('DOMContentLoaded', addHtmxListener);
    }

})();
