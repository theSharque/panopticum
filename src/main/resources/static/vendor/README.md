# Vendor assets (offline / closed-circuit deployment)

All external JS/CSS/fonts are stored locally for use in environments without internet access.

## Contents

- **htmx/** — HTMX 1.9.10
- **prism/** — Prism.js 1.29.0 (core, json, tomorrow theme)
- **codemirror/** — CodeMirror 6 view styles
- **fonts/** — Inter and JetBrains Mono (latin + cyrillic subsets)

## Rebuilding detail-editor.bundle.js

When updating CodeMirror or changing `detail-editor-src.js`:

```bash
npm install
npm run build:detail-editor
```

Output: `static/js/detail-editor.bundle.js`

## Updating vendor files

Download from the same CDN URLs used previously (see git history) and replace the files.
