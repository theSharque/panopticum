(function() {
    var feedbackMs = 1100;

    document.addEventListener('click', function(e) {
        var btn = e.target.closest('.breadcrumbs-copy-btn');
        if (!btn) {
            return;
        }
        var text = btn.getAttribute('data-copy-text');
        if (!text) {
            return;
        }
        if (!navigator.clipboard || !navigator.clipboard.writeText) {
            return;
        }
        if (btn._copyFeedbackTimer) {
            clearTimeout(btn._copyFeedbackTimer);
            btn._copyFeedbackTimer = null;
        }
        navigator.clipboard.writeText(text).then(function() {
            btn.classList.add('breadcrumbs-copy-btn--copied');
            btn._copyFeedbackTimer = setTimeout(function() {
                btn.classList.remove('breadcrumbs-copy-btn--copied');
                btn._copyFeedbackTimer = null;
            }, feedbackMs);
        });
    });
})();
