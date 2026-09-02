import { access, readFile, readdir } from 'node:fs/promises';
import { extname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../out/', import.meta.url));
const contentRoot = fileURLToPath(new URL('../content/docs/', import.meta.url));
const errors = [];

async function walk(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const files = await Promise.all(entries.map(async (entry) => {
    const path = join(directory, entry.name);
    return entry.isDirectory() ? walk(path) : path;
  }));
  return files.flat();
}

async function exists(path) {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

for (const expected of [
  'index.html',
  'quick-start.html',
  'features/stacks.html',
  'quick-start/install-komodo.html',
  'quick-start/add-another-server.html',
  'administration/container-deployment.html',
  'screenshots.html',
  'logo.png',
  'screenshots/Dark-Dashboard.png',
]) {
  if (!(await exists(join(root, expected)))) errors.push(`missing static output: ${expected}`);
}

const searchOutputs = ['api/search', 'api/search.json', 'api/search/index.html'];
if (!(await Promise.all(searchOutputs.map((path) => exists(join(root, path))))).some(Boolean)) {
  errors.push(`missing static search index (checked ${searchOutputs.join(', ')})`);
}

if (await exists(join(root, 'CNAME'))) errors.push('static output must not contain CNAME');

const sourceFiles = (await walk(contentRoot)).filter((file) => ['.md', '.mdx'].includes(extname(file)));
for (const source of sourceFiles) {
  let route = relative(contentRoot, source).replaceAll('\\', '/').replace(/\.(?:md|mdx)$/, '');
  if (route.endsWith('/index')) route = route.slice(0, -'/index'.length);
  if (!route) continue;
  if (!(await exists(join(root, `${route}.html`)))) errors.push(`missing exported page: /${route}`);
}

const outputFiles = await walk(root);
for (const file of outputFiles.filter((path) => path.endsWith('.html'))) {
  const html = await readFile(file, 'utf8');
  if (/:::(?:note|info|tip|warning|danger)\b/.test(html)) {
    errors.push(`${relative(root, file)} contains a raw Docusaurus callout fence`);
  }
  if (/(?:href|src)=["']\/docs(?:\/|["'])/.test(html)) {
    errors.push(`${relative(root, file)} contains a legacy /docs application or asset link`);
  }
  if (/DOCSITE_(?:URL|BASE_URL)/.test(html)) {
    errors.push(`${relative(root, file)} contains a legacy DOCSITE build argument`);
  }
}

if (errors.length > 0) {
  console.error(errors.join('\n'));
  process.exit(1);
}

console.log(`Validated ${sourceFiles.length} exported routes, root assets, deep links, and static search output.`);
