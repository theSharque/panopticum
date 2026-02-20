(function () {
    'use strict';

    const PREFIX = 'panopticum:qh:';
    const MAX_QUERIES = 50;
    const CONTEXT_NAMES = ['dbName', 'schema', 'keyspaceName', 'collection', 'indexName'];

    function buildStorageKey(form) {
        const postUrl = form.getAttribute('hx-post') || form.getAttribute('action') || '';
        const path = postUrl.replace(/^https?:\/\/[^/]+/, '').replace(/\?.*$/, '') || '/';
        const parts = [path];
        CONTEXT_NAMES.forEach(function (name) {
            const input = form.querySelector('input[name="' + name + '"]');
            if (input && input.value) {
                parts.push(input.value);
            }
        });
        return PREFIX + parts.join(':');
    }

    function loadHistory(key) {
        try {
            const raw = localStorage.getItem(key);
            return raw ? JSON.parse(raw) : [];
        } catch (e) {
            return [];
        }
    }

    function saveHistory(key, queries) {
        try {
            localStorage.setItem(key, JSON.stringify(queries));
        } catch (e) {
        }
    }

    function addQuery(key, query) {
        const trimmed = (query || '').trim();
        if (!trimmed) return;
        let queries = loadHistory(key);
        queries = queries.filter(function (q) { return q !== trimmed; });
        queries.unshift(trimmed);
        queries = queries.slice(0, MAX_QUERIES);
        saveHistory(key, queries);
    }

    function removeQuery(key, query) {
        let queries = loadHistory(key).filter(function (q) { return q !== query; });
        saveHistory(key, queries);
    }

    function clearHistory(key) {
        saveHistory(key, []);
    }

    function getQueryTextarea(form) {
        return form.querySelector('textarea[name="sql"], textarea[name="query"]');
    }

    function getClockIconSvg() {
        return '<svg class="icon-clock" xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>';
    }

    function getDeleteIconSvg() {
        return '<svg class="icon-delete" xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>';
    }

    function truncate(str, len) {
        if (!str || str.length <= len) return str;
        return str.substring(0, len).trim() + '…';
    }

    function createDropdown(form, key, btn) {
        const textarea = getQueryTextarea(form);
        if (!textarea) return null;

        const wrapper = document.createElement('div');
        wrapper.className = 'query-history-dropdown-wrapper';
        wrapper.style.position = 'relative';

        const dropdown = document.createElement('div');
        dropdown.className = 'query-history-dropdown';
        dropdown.setAttribute('hidden', '');

        function render() {
            const queries = loadHistory(key);
            dropdown.innerHTML = '';
            if (queries.length === 0) {
                const empty = document.createElement('div');
                empty.className = 'query-history-empty';
                empty.textContent = 'No saved queries';
                dropdown.appendChild(empty);
            } else {
                queries.forEach(function (q) {
                    const item = document.createElement('div');
                    item.className = 'query-history-item';
                    const text = document.createElement('span');
                    text.className = 'query-history-item-text';
                    text.textContent = truncate(q, 80);
                    text.title = q;
                    const delBtn = document.createElement('button');
                    delBtn.type = 'button';
                    delBtn.className = 'query-history-item-delete';
                    delBtn.innerHTML = getDeleteIconSvg();
                    delBtn.setAttribute('aria-label', 'Delete');
                    delBtn.addEventListener('click', function (ev) {
                        ev.stopPropagation();
                        removeQuery(key, q);
                        render();
                    });
                    text.addEventListener('click', function () {
                        textarea.value = q;
                        dropdown.setAttribute('hidden', '');
                        dropdown.classList.remove('query-history-dropdown-visible');
                    });
                    item.appendChild(text);
                    item.appendChild(delBtn);
                    dropdown.appendChild(item);
                });
                const clearLink = document.createElement('button');
                clearLink.type = 'button';
                clearLink.className = 'query-history-clear';
                clearLink.textContent = 'Clear all';
                clearLink.addEventListener('click', function () {
                    clearHistory(key);
                    render();
                });
                dropdown.appendChild(clearLink);
            }
        }

        btn.addEventListener('click', function (ev) {
            ev.preventDefault();
            ev.stopPropagation();
            const isVisible = dropdown.classList.contains('query-history-dropdown-visible');
            if (isVisible) {
                dropdown.classList.remove('query-history-dropdown-visible');
                dropdown.setAttribute('hidden', '');
            } else {
                render();
                dropdown.removeAttribute('hidden');
                dropdown.classList.add('query-history-dropdown-visible');
            }
        });

        function closeDropdown() {
            dropdown.classList.remove('query-history-dropdown-visible');
            dropdown.setAttribute('hidden', '');
        }

        document.addEventListener('click', function (ev) {
            if (!wrapper.contains(ev.target)) {
                closeDropdown();
            }
        });
        document.addEventListener('keydown', function (ev) {
            if (ev.key === 'Escape') closeDropdown();
        });

        form.appendChild(dropdown);
        return dropdown;
    }

    function injectHistoryButton(form) {
        const textarea = getQueryTextarea(form);
        if (!textarea) return;

        const executeBtn = form.querySelector('.query-execute');
        if (!executeBtn) return;

        const key = buildStorageKey(form);
        if (!key || key === PREFIX) return;

        const actions = document.createElement('div');
        actions.className = 'query-actions';

        const historyBtn = document.createElement('button');
        historyBtn.type = 'button';
        historyBtn.className = 'query-history-btn';
        historyBtn.setAttribute('aria-label', 'Query history');
        historyBtn.setAttribute('title', 'Query history');
        historyBtn.innerHTML = getClockIconSvg();

        executeBtn.parentNode.insertBefore(actions, executeBtn);
        actions.appendChild(historyBtn);
        actions.appendChild(executeBtn);

        createDropdown(form, key, historyBtn);
    }

    function init() {
        document.querySelectorAll('.query-form').forEach(function (form) {
            if (form.querySelector('.query-textarea') && !form.querySelector('.query-history-btn')) {
                injectHistoryButton(form);
            }
        });
    }

    document.addEventListener('DOMContentLoaded', init);

    document.addEventListener('htmx:afterSwap', function (ev) {
        const target = ev.detail && ev.detail.target ? ev.detail.target : ev.target;
        if (!target || typeof target.querySelector !== 'function') return;

        const hasError = target.querySelector && target.querySelector('.query-error');
        if (hasError) return;

        const targetId = target.id;
        if (!targetId) return;

        const selector = 'form.query-form[hx-target="#' + targetId + '"]';
        const form = document.querySelector(selector);
        if (!form) return;

        const textarea = getQueryTextarea(form);
        if (!textarea) return;

        const query = (textarea.value || '').trim();
        if (!query) return;

        const key = buildStorageKey(form);
        if (key && key !== PREFIX) {
            addQuery(key, query);
        }
    });

})();
