import { EditorView, keymap, panels } from '@codemirror/view';
import { EditorState } from '@codemirror/state';
import { syntaxHighlighting, defaultHighlightStyle } from '@codemirror/language';
import { json } from '@codemirror/lang-json';
import { oneDark } from '@codemirror/theme-one-dark';
import { search, searchKeymap, highlightSelectionMatches } from '@codemirror/search';

const CM_REPLACED_CLASS = 'cm-replaced';
const SEARCH_HOST_ID = 'cm-search-host';

function isDark() {
    return document.body.getAttribute('data-theme') !== 'light';
}

function getSearchHost() {
    var host = document.getElementById(SEARCH_HOST_ID);
    if (host) {
        return host;
    }
    host = document.createElement('div');
    host.id = SEARCH_HOST_ID;
    host.className = 'cm-search-host';
    document.body.appendChild(host);
    return host;
}

function searchPanelTheme() {
    return EditorView.theme({
        '&': { fontSize: '13px' },
        '&.cm-editor': { minHeight: '120px' },
        '&.cm-scroller': { fontFamily: 'var(--font-mono), "JetBrains Mono", monospace' },
        '.cm-selectionMatch': {
            backgroundColor: 'color-mix(in srgb, var(--accent, #3b82f6) 28%, transparent)'
        }
    });
}

function initEditor(textarea) {
    if (textarea.dataset.cmInitialized === 'true') {
        return;
    }
    var content = textarea.value || '';
    var wrapper = document.createElement('div');
    wrapper.className = 'detail-json cm-editor-wrapper';
    textarea.parentNode.insertBefore(wrapper, textarea);
    wrapper.appendChild(textarea);
    textarea.classList.add(CM_REPLACED_CLASS);
    textarea.style.position = 'absolute';
    textarea.style.left = '-9999px';
    textarea.style.width = '1px';
    textarea.style.height = '1px';
    textarea.style.opacity = '0';
    textarea.style.pointerEvents = 'none';
    textarea.style.overflow = 'hidden';

    var extensions = [
        isDark() ? oneDark : syntaxHighlighting(defaultHighlightStyle, { fallback: true }),
        json(),
        EditorView.lineWrapping,
        panels({ topContainer: getSearchHost() }),
        search({ top: true }),
        highlightSelectionMatches(),
        keymap.of(searchKeymap),
        searchPanelTheme()
    ];

    var state = EditorState.create({
        doc: content,
        extensions: extensions
    });
    var view = new EditorView({
        state: state,
        parent: wrapper
    });
    textarea.dataset.cmInitialized = 'true';
    textarea.value = '';

    var form = textarea.closest('form');
    if (form) {
        form.addEventListener('submit', function () {
            textarea.value = view.state.doc.toString();
        });
    }
}

function init(container) {
    var root = container || document;
    var textareas = root.querySelectorAll('.detail-form .detail-json-textarea');
    textareas.forEach(function (ta) {
        if (!ta.classList.contains(CM_REPLACED_CLASS)) {
            initEditor(ta);
        }
    });
}

function run() {
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', function () {
            init();
        });
    } else {
        init();
    }
    function addHtmxListener() {
        if (document.body) {
            document.body.addEventListener('htmx:afterSwap', function (ev) {
                var target = ev.detail && ev.detail.target;
                if (target && target.querySelectorAll) {
                    init(target);
                }
            });
        }
    }
    if (document.body) {
        addHtmxListener();
    } else {
        document.addEventListener('DOMContentLoaded', addHtmxListener);
    }
}

run();
