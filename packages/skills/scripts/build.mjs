import { mkdir, readdir, readFile, writeFile, copyFile, chmod } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { join } from 'node:path';

const root = fileURLToPath(new URL('../', import.meta.url));
const metadata = JSON.parse(await readFile(join(root, 'package.json'), 'utf8'));
if (metadata.name !== '@hanger-source/dsh-reactive-skills' || typeof metadata.version !== 'string' || !metadata.version) {
  throw new Error('REACTIVE_DEVELOPER_SKILL_METADATA_INVALID');
}
for (const [name, version] of Object.entries(metadata.peerDependencies)) {
  if (version !== metadata.version || metadata.peerDependenciesMeta[name]?.optional !== true) {
    throw new Error(`REACTIVE_DEVELOPER_SKILL_VERSION_MISMATCH: ${name}`);
  }
}

async function emitTemplates(source, destination) {
  await mkdir(destination, { recursive: true });
  for (const entry of await readdir(source, { withFileTypes: true })) {
    const input = join(source, entry.name);
    const output = join(destination, entry.name);
    if (entry.isDirectory()) await emitTemplates(input, output);
    else if (entry.isFile() && entry.name.endsWith('.md')) {
      const template = await readFile(input, 'utf8');
      if (!template.includes('{{PACKAGE_VERSION}}')) throw new Error(`REACTIVE_DEVELOPER_SKILL_VERSION_MISSING: ${input}`);
      const content = template.replaceAll('{{PACKAGE_VERSION}}', metadata.version);
      if (/\{\{[A-Z_]+\}\}/u.test(content)) throw new Error(`REACTIVE_DEVELOPER_SKILL_TEMPLATE_UNRESOLVED: ${input}`);
      await writeFile(output, content);
    } else throw new Error(`REACTIVE_DEVELOPER_SKILL_TEMPLATE_INVALID: ${input}`);
  }
}
await emitTemplates(join(root, 'templates'), join(root, 'skills'));
await mkdir(join(root, 'dist'), { recursive: true });
for (const name of ['index.mjs', 'index.d.ts', 'cli.mjs']) await copyFile(join(root, 'src', name), join(root, 'dist', name));
await chmod(join(root, 'dist', 'cli.mjs'), 0o755);
console.log(`@hanger-source/dsh-reactive-skills ${metadata.version}: skills/dsh-reactive/SKILL.md`);
