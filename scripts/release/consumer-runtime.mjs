import assert from 'node:assert/strict';
import { readFile, readdir, realpath, lstat, access } from 'node:fs/promises';
import { join, relative, isAbsolute, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

const execute = promisify(execFile);
const expected = JSON.parse(await readFile(new URL('./expected.json', import.meta.url), 'utf8'));
const root = dirname(fileURLToPath(import.meta.url));
for (const name of expected.packages) {
  const directory = join(root, 'node_modules', name);
  assert.equal((await lstat(directory)).isSymbolicLink(), false, name + ' 必须为物理安装');
  assert.equal(await realpath(directory), directory);
  const pkg = JSON.parse(await readFile(join(directory, 'package.json'), 'utf8'));
  const receipt = JSON.parse(await readFile(join(directory, 'release.json'), 'utf8'));
  assert.equal(pkg.version, expected.version);
  assert.equal(receipt.package, name); assert.equal(receipt.version, expected.version);
  assert.equal(receipt.commit, expected.commit); assert.equal(receipt.tag, expected.tag);
  assert.equal(receipt.registry, expected.registry);
  const imported = await import(name);
  assert.ok(Object.keys(imported).length > 0, name + ' 公开入口必须实际加载');
}
if (expected.kind === 'backend') {
  const program = await import('@hanger-source/dsh-reactive/program');
  assert.equal(typeof program.implementProgram, 'function');
  const skills = await import('@hanger-source/dsh-reactive-dsh/skills');
  assert.equal(typeof skills.createNativeSkills, 'function');
}
const skill = await import('@hanger-source/dsh-reactive-skills');
const manifest = skill.getDeveloperSkill();
assert.equal(manifest.name, 'dsh-reactive'); assert.equal(manifest.version, expected.version);
const directory = await realpath(manifest.directory);
const entrypoint = await realpath(manifest.entrypoint);
const child = relative(directory, entrypoint);
assert.ok(child && child !== '..' && !child.startsWith('../') && !isAbsolute(child));
await access(entrypoint);
const references = (await readdir(join(directory, 'references'))).filter(file => file.endsWith('.md'));
assert.equal(references.length, 5);
for (const file of references) assert.ok((await readFile(join(directory, 'references', file), 'utf8')).length > 0);
const installed = JSON.parse(await readFile(join(root, 'node_modules/@hanger-source/dsh-reactive-skills/package.json'), 'utf8'));
for (const [name, version] of Object.entries(installed.peerDependencies)) {
  assert.equal(version, expected.version); assert.equal(installed.peerDependenciesMeta[name].optional, true);
}
const command = await execute(process.execPath, [join(root, 'node_modules/.bin/dsh-reactive-skill')], { cwd: root });
assert.deepEqual(JSON.parse(command.stdout), manifest);
console.log(JSON.stringify({ kind: expected.kind, source: expected.source, commit: expected.commit,
  version: expected.version, packages: expected.packages, developerSkill: { ...manifest, references } }));
