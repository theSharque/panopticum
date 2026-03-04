import * as esbuild from 'esbuild';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, '..');

await esbuild.build({
  entryPoints: [join(root, 'src/main/resources/static/js/detail-editor-src.js')],
  bundle: true,
  format: 'iife',
  outfile: join(root, 'src/main/resources/static/js/detail-editor.bundle.js'),
  minify: true
});
