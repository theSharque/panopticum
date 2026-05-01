import { EditorView, placeholder } from '@codemirror/view';
import { EditorState } from '@codemirror/state';
import { syntaxHighlighting, defaultHighlightStyle } from '@codemirror/language';
import { sql } from '@codemirror/lang-sql';
import { json } from '@codemirror/lang-json';
import { oneDark } from '@codemirror/theme-one-dark';

const CM_REPLACED_CLASS = 'cm-replaced';

function isDark() {
    return document.body.getAttribute('data-theme') !== 'light';
}

function initEditor(textarea) {
    if (textarea.dataset.cmInitialized === 'true') {
        return;
    }

    var lang = textarea.dataset.lang || 'sql';
    var content = textarea.value || '';
    var parent = textarea.parentNode;
    var wrapper = document.createElement('div');
    wrapper.className = 'query-editor cm-editor-wrapper';
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

    var languageExtension = lang === 'json' ? json() : sql();
    var placeholderText = textarea.getAttribute('placeholder') || '';
    var extensions = [
        isDark() ? oneDark : syntaxHighlighting(defaultHighlightStyle, { fallback: true }),
        languageExtension,
        EditorView.lineWrapping,
        EditorView.theme({
            '&': { fontSize: '13px' },
            '&.cm-editor': { minHeight: '120px' },
            '&.cm-scroller': { fontFamily: 'var(--font-mono), "JetBrains Mono", monospace' },
            '.cm-placeholder': {
                color: 'var(--text-dim)',
                opacity: '0.85'
            }
        })
    ];

    if (placeholderText) {
        extensions.push(placeholder(placeholderText));
    }

    var state = EditorState.create({
        doc: content,
        extensions: extensions
    });

    var view = new EditorView({
        state: state,
        parent: wrapper
    });

    textarea.dataset.cmInitialized = 'true';
    textarea._panopticumCmView = view;
    textarea.value = '';

    var form = textarea.closest('form');
    if (form) {
        form.addEventListener(
            'submit',
            function () {
                textarea.value = view.state.doc.toString();
            },
            true
        );
    }

    textarea.addEventListener('panopticum:query-apply', function (ev) {
        if (ev.detail && ev.detail.value) {
            view.dispatch({
                changes: {
                    from: 0,
                    to: view.state.doc.length,
                    insert: ev.detail.value
                }
            });
        }
    });
}

function init(container) {
    var root = container || document;
    var textareas = root.querySelectorAll('.query-textarea[data-lang]');
    textareas.forEach(function (ta) {
        if (!ta.classList.contains(CM_REPLACED_CLASS)) {
            initEditor(ta);
        }
    });
}

function patchCmIntoHtmxRequest(ev) {
    var elt = ev.detail && ev.detail.elt;
    if (!elt) {
        return;
    }
    var form = elt.tagName === 'FORM' ? elt : elt.closest('form');
    if (!form) {
        return;
    }
    var ta = form.querySelector('textarea.cm-replaced[name="sql"], textarea.cm-replaced[name="query"]');
    if (!ta || !ta._panopticumCmView) {
        return;
    }
    var text = ta._panopticumCmView.state.doc.toString();
    ta.value = text;
    var params = ev.detail.parameters;
    if (!params) {
        return;
    }
    if (ta.getAttribute('name') === 'sql') {
        params.sql = text;
    }
    if (ta.getAttribute('name') === 'query') {
        params.query = text;
    }
}

function run() {
    if (document.body) {
        document.body.addEventListener('htmx:configRequest', patchCmIntoHtmxRequest);
    }

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
