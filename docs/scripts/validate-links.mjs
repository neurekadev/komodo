import { access, readFile, readdir } from 'node:fs/promises';
import { dirname, extname, join, normalize, relative, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

const docsRoot = fileURLToPath(new URL('../content/docs/', import.meta.url));
const publicRoot = fileURLToPath(new URL('../public/', import.meta.url));
const errors = [];

async function walk(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const files = await Promise.all(entries.map(async (entry) => {
    const path = join(directory, entry.name);
    return entry.isDirectory() ? walk(path) : path;
  }));
  return files.flat();
}

function routeFor(file) {
  const sourcePath = relative(docsRoot, file).split(sep).join('/');
  const withoutExtension = sourcePath.slice(0, -extname(sourcePath).length);
  const route = withoutExtension.endsWith('/index')
    ? withoutExtension.slice(0, -'/index'.length)
    : withoutExtension;
  return `/${route}`.replace(/\/$/, '') || '/';
}

function slugify(value) {
  return value
    .replace(/<[^>]+>/g, '')
    .replace(/!?\[([^\]]*)\]\([^)]*\)/g, '$1')
    .replace(/[`*_~]/g, '')
    .trim()
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s-]/gu, '')
    .replace(/\s+/g, '-');
}

function headingsFor(content) {
  const withoutCode = content.replace(/```[\s\S]*?```/g, '');
  const counts = new Map();
  const headings = new Set();

  for (const match of withoutCode.matchAll(/^#{1,6}\s+(.+?)\s*$/gm)) {
    const explicit = match[1].match(/\[#([\w-]+)\]\s*$/)?.[1];
    const base = explicit ?? slugify(match[1].replace(/\s+\[(?:!?)toc\]\s*$/g, ''));
    const count = counts.get(base) ?? 0;
    headings.add(count === 0 ? base : `${base}-${count}`);
    counts.set(base, count + 1);
  }

  return headings;
}

function splitTarget(target) {
  const [withoutHash, fragment = ''] = target.split('#', 2);
  return { path: withoutHash.split('?', 1)[0], fragment: decodeURIComponent(fragment) };
}

async function exists(path) {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

const files = (await walk(docsRoot)).filter((file) => ['.md', '.mdx'].includes(extname(file)));
const pages = new Map();
const fileToRoute = new Map();

for (const file of files) {
  const content = await readFile(file, 'utf8');
  const route = routeFor(file);
  pages.set(route, { file, headings: headingsFor(content) });
  fileToRoute.set(normalize(file), route);
}

for (const file of files) {
  const content = await readFile(file, 'utf8');
  const currentRoute = fileToRoute.get(normalize(file));
  if (/^:::(?:note|info|tip|warning|danger)?\b/m.test(content)) {
    errors.push(`${relative(docsRoot, file)}: contains an unconverted Docusaurus callout fence`);
  }
  const markdownTargets = [...content.matchAll(/(!?)\[[^\]]*\]\(([^)\s]+)(?:\s+['"][^'"]*['"])?\)/g)]
    .map((match) => ({ target: match[2], image: match[1] === '!' }));
  const jsxTargets = [...content.matchAll(/<(?:a|img)\b[^>]*?\b(href|src)=["']([^"']+)["'][^>]*>/g)]
    .map((match) => ({ target: match[2], image: match[1] === 'src' }));

  for (const { target, image } of [...markdownTargets, ...jsxTargets]) {
    if (/^(?:https?:|mailto:|tel:|data:)/.test(target)) continue;
    if (target.includes('<') || target.includes('{')) continue;

    const { path, fragment } = splitTarget(target);
    if (path === '/docs' || path.startsWith('/docs/')) {
      errors.push(`${relative(docsRoot, file)}: legacy /docs link ${target}`);
      continue;
    }

    let destinationRoute = currentRoute;

    if (path.startsWith('/')) {
      if (path === '/') {
        destinationRoute = '/';
      } else if (image || path === '/favicon.ico' || path === '/logo.png' || path.startsWith('/screenshots/')) {
        const asset = resolve(publicRoot, `.${path}`);
        if (!asset.startsWith(publicRoot) || !(await exists(asset))) {
          errors.push(`${relative(docsRoot, file)}: missing asset ${target}`);
        }
        continue;
      } else {
        destinationRoute = path.replace(/\/$/, '');
      }
    } else if (path) {
      const sourceCandidate = resolve(dirname(file), path);
      const sourceOptions = extname(sourceCandidate)
        ? [sourceCandidate]
        : [`${sourceCandidate}.md`, `${sourceCandidate}.mdx`, join(sourceCandidate, 'index.md'), join(sourceCandidate, 'index.mdx')];
      const sourceFile = sourceOptions.find((candidate) => fileToRoute.has(normalize(candidate)));

      if (sourceFile) {
        destinationRoute = fileToRoute.get(normalize(sourceFile));
      } else {
        const base = currentRoute.endsWith('/') ? currentRoute : `${currentRoute}/`;
        destinationRoute = new URL(path, `https://docs.invalid${base}`).pathname.replace(/\/$/, '');
      }
    }

    if (destinationRoute === '/') {
      if (fragment) errors.push(`${relative(docsRoot, file)}: homepage anchor cannot be verified: ${target}`);
      continue;
    }

    const page = pages.get(destinationRoute);
    if (!page) {
      errors.push(`${relative(docsRoot, file)}: missing page ${target}`);
      continue;
    }
    if (fragment && !page.headings.has(fragment)) {
      errors.push(`${relative(docsRoot, file)}: missing anchor #${fragment} on ${destinationRoute}`);
    }
  }

  for (const match of content.matchAll(/!\[([^\]]*)\]\(/g)) {
    if (!match[1].trim()) errors.push(`${relative(docsRoot, file)}: image is missing alt text`);
  }
  for (const match of content.matchAll(/<img\b([^>]*)>/g)) {
    if (!/\balt=["'][^"']+["']/.test(match[1])) {
      errors.push(`${relative(docsRoot, file)}: JSX image is missing descriptive alt text`);
    }
  }
}

if (errors.length > 0) {
  console.error(errors.join('\n'));
  process.exit(1);
}

console.log(`Validated ${files.length} documentation pages, their links, anchors, and image paths.`);
