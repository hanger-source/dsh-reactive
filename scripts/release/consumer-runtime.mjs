import assert from 'node:assert/strict';
import { readFile, realpath, lstat } from 'node:fs/promises';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { checkDeveloperSkill } from './consumer-skill.mjs';

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
  const tools = await import('@hanger-source/dsh-reactive-dsh/tools');
  assert.equal(typeof tools.createNativeTools, 'function');
}
const developerSkill = await checkDeveloperSkill(expected.version);
console.log(JSON.stringify({ kind: expected.kind, source: expected.source, commit: expected.commit,
  version: expected.version, packages: expected.packages, developerSkill }));
