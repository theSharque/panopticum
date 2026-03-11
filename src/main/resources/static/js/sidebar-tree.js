(function () {
    var STORAGE_PREFIX = 'panopticum-tree-';

    function buildTree(sourceEl) {
        var root = { items: [], children: {} };
        var items = sourceEl.querySelectorAll('.sidebar-pg[data-name]');
        var itemMap = {};
        for (var i = 0; i < items.length; i++) {
            var el = items[i];
            var name = el.getAttribute('data-name');
            if (!name) continue;
            itemMap[name] = el;
        }

        var names = Object.keys(itemMap).sort();
        for (var j = 0; j < names.length; j++) {
            var fullName = names[j];
            var parts = fullName.split('/');
            var node = root;
            for (var k = 0; k < parts.length; k++) {
                var segment = parts[k];
                if (k === parts.length - 1) {
                    node.items.push({ name: fullName, el: itemMap[fullName], leaf: segment });
                } else {
                    if (!node.children[segment]) {
                        node.children[segment] = { items: [], children: {} };
                    }
                    node = node.children[segment];
                }
            }
        }
        return root;
    }

    function getStorageKey(path) {
        return STORAGE_PREFIX + encodeURIComponent(path || '');
    }

    function isOpen(path) {
        try {
            var stored = localStorage.getItem(getStorageKey(path));
            return stored === 'open';
        } catch (e) {
            return false;
        }
    }

    function setOpen(path, open) {
        try {
            localStorage.setItem(getStorageKey(path), open ? 'open' : 'closed');
        } catch (e) {}
    }

    function renderFolder(node, path, depth, container) {
        var items = node.items || [];
        var children = node.children || {};
        var childKeys = Object.keys(children).sort();

        for (var i = 0; i < items.length; i++) {
            var item = items[i];
            var link = item.el.querySelector('.sidebar-link');
            if (link) {
                link.textContent = item.leaf;
            }
            container.appendChild(item.el);
        }

        for (var j = 0; j < childKeys.length; j++) {
            var key = childKeys[j];
            var childNode = children[key];
            var childPath = path ? path + '/' + key : key;
            var open = isOpen(childPath);

            var folder = document.createElement('div');
            folder.className = 'sidebar-folder' + (open ? '' : ' collapsed');

            var header = document.createElement('div');
            header.className = 'sidebar-folder-header';
            header.setAttribute('role', 'button');
            header.setAttribute('tabindex', '0');
            header.setAttribute('aria-expanded', open ? 'true' : 'false');

            var toggle = document.createElement('span');
            toggle.className = 'sidebar-folder-toggle';
            toggle.setAttribute('aria-hidden', 'true');
            toggle.textContent = '\u25B6';

            var label = document.createElement('span');
            label.className = 'sidebar-folder-label';
            label.textContent = key;

            header.appendChild(toggle);
            header.appendChild(label);

            var childrenContainer = document.createElement('div');
            childrenContainer.className = 'sidebar-folder-children';

            (function (f, h, t, p) {
                h.addEventListener('click', function () {
                    var isCurrentlyOpen = !f.classList.contains('collapsed');
                    setOpen(p, !isCurrentlyOpen);
                    f.classList.toggle('collapsed', isCurrentlyOpen);
                    h.setAttribute('aria-expanded', isCurrentlyOpen ? 'false' : 'true');
                    t.style.transform = isCurrentlyOpen ? 'rotate(0deg)' : 'rotate(90deg)';
                });
            })(folder, header, toggle, childPath);

            if (open) {
                toggle.style.transform = 'rotate(90deg)';
            }

            renderFolder(childNode, childPath, depth + 1, childrenContainer);

            folder.appendChild(header);
            folder.appendChild(childrenContainer);
            container.appendChild(folder);
        }
    }

    function initSidebarTree() {
        var source = document.getElementById('sidebar-conn-source');
        var tree = document.getElementById('sidebar-tree');
        if (!source || !tree) return;

        tree.innerHTML = '';
        var root = buildTree(source);
        renderFolder(root, '', 0, tree);
    }

    function initHtmxErrorHandler() {
        document.body.addEventListener('htmx:responseError', function (e) {
            var xhr = e.detail && e.detail.xhr;
            if (!xhr || xhr.status !== 400) return;
            var url = xhr.responseURL || '';
            if (url.indexOf('/settings/add-') === -1) return;

            var msgEl = document.getElementById('validation-messages');
            if (!msgEl) return;

            var text = '';
            try {
                var body = {};
                var raw = xhr.responseText || '';
                if (raw) {
                    try {
                        body = JSON.parse(raw);
                    } catch (e) {
                        body = { message: raw };
                    }
                }
                var key = body.message || (body._embedded && body._embedded.errors && body._embedded.errors[0] && body._embedded.errors[0].message) || '';
                if (key === 'connection.nameTrailingSlash') {
                    text = msgEl.dataset.nameTrailingSlash || key;
                } else if (key === 'connection.nameDuplicate') {
                    text = msgEl.dataset.nameDuplicate || key;
                } else {
                    text = key || 'Validation error';
                }
            } catch (err) {
                text = 'Validation error';
            }

            var blocks = document.querySelectorAll('.connection-form-block');
            for (var i = 0; i < blocks.length; i++) {
                if (!blocks[i].hidden) {
                    var result = blocks[i].querySelector('.connection-test-result');
                    if (result) {
                        result.innerHTML = '<span class="connection-test-error">' + escapeHtml(text) + '</span>';
                        result.style.display = 'block';
                    }
                    break;
                }
            }
        });
    }

    function escapeHtml(s) {
        var div = document.createElement('div');
        div.textContent = s;
        return div.innerHTML;
    }

    function init() {
        initSidebarTree();
        initHtmxErrorHandler();
        var body = document.body;
        if (body) {
            body.addEventListener('htmx:afterSettle', function (e) {
                if (e.detail && e.detail.target && e.detail.target.id === 'sidebar') {
                    initSidebarTree();
                }
            });
        }
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
