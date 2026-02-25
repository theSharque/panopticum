document.addEventListener('DOMContentLoaded', function () {
    const textareas = document.querySelectorAll('.detail-edit-textarea, .detail-json-textarea');

    function adjustHeight(el) {
        el.style.height = 'auto';
        el.style.height = el.scrollHeight + 24 + 'px';
    }

    textareas.forEach(function (el) {
        adjustHeight(el);
        el.addEventListener('input', function () {
            adjustHeight(el);
        });
    });
});
