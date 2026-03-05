(function () {
    'use strict';

    function highlightAll(container) {
        var root = container || document;
        var blocks = root.querySelectorAll('pre.detail-json code.language-json, pre.detail-value.detail-json code.language-json');
        blocks.forEach(function (el) {
            if (typeof Prism !== 'undefined' && Prism.highlightElement) {
                Prism.highlightElement(el);
            }
        });
    }

    function init() {
        highlightAll();
    }

    document.addEventListener('DOMContentLoaded', init);

    function addHtmxListener() {
        if (document.body) {
            document.body.addEventListener('htmx:afterSwap', function (ev) {
                var target = ev.detail && ev.detail.target;
                if (target && target.querySelectorAll) {
                    highlightAll(target);
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
