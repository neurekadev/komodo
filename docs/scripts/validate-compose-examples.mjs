import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';

const guides = [
  'quick-start/install-komodo.mdx',
  'administration/databases/ferretdb.mdx',
  'quick-start/add-another-server.mdx',
];

for (const guide of guides) {
  const content = await readFile(new URL(`../content/docs/${guide}`, import.meta.url), 'utf8');
  const directory = await mkdtemp(join(tmpdir(), 'komodo-compose-'));
  try {
    for (const name of ['compose.yaml', '.env']) {
      const blocks = [...content.matchAll(/^```\w+ title="([^"]+)"\r?\n([\s\S]*?)^```\s*$/gm)]
        .filter((match) => match[1] === name);
      if (blocks.length !== 1) throw new Error(`${guide}: expected one ${name} example`);
      await writeFile(join(directory, name), blocks[0][2]);
    }
    const result = spawnSync('docker', ['compose', '-p', 'komodo', '--project-directory', directory, 'config', '--quiet'], {
      stdio: 'inherit',
      // Validate only the documented inputs, without inheriting local overrides.
      env: Object.fromEntries(Object.entries(process.env).filter(([name]) => !/^(COMPOSE_|KOMODO_|PERIPHERY_|MONGO_|POSTGRES_|FERRETDB_)/.test(name))),
    });
    if (result.error) throw result.error;
    if (result.status !== 0) throw new Error(`${guide}: Compose validation failed`);
    console.log(`Validated ${guide}`);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}
