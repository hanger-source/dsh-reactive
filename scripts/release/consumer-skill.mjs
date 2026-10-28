import assert from 'node:assert/strict';
import { lstat, readFile, readdir, realpath } from 'node:fs/promises';
import { dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

const execute = promisify(execFile);
const root = dirname(fileURLToPath(import.meta.url));
const within = (parent, child) => {
  const path = relative(parent, child);
  return path !== '..' && !path.startsWith('..' + sep) && !isAbsolute(path);
};

/** 在实际仓外安装中消费版本、Skill 路径与 npm bin；不读取仓库模板或发行回执。 */
export async function checkDeveloperSkill(version) {
  assert.ok(typeof version === 'string' && version, '必须明确本次消费的 Skill 版本');
  const packageRoot = join(root, 'node_modules/@hanger-source/dsh-reactive-skills');
  assert.equal((await lstat(packageRoot)).isSymbolicLink(), false, 'Skill 必须为物理安装');
  assert.equal(await realpath(packageRoot), packageRoot);
  const installed = JSON.parse(await readFile(join(packageRoot, 'package.json'), 'utf8'));
  assert.equal(installed.name, '@hanger-source/dsh-reactive-skills');
  assert.equal(installed.version, version);

  const skill = await import('@hanger-source/dsh-reactive-skills');
  const manifest = skill.getDeveloperSkill();
  assert.equal(manifest.name, 'dsh-reactive');
  assert.equal(manifest.version, installed.version);
  assert.ok(isAbsolute(manifest.directory) && isAbsolute(manifest.entrypoint));
  const directory = await realpath(manifest.directory), entrypoint = await realpath(manifest.entrypoint);
  assert.ok(within(packageRoot, directory) && within(directory, entrypoint) && directory !== entrypoint,
    '公开 Skill 入口必须位于实际安装包内');
  const entry = await readFile(entrypoint, 'utf8');
  const references = (await readdir(join(directory, 'references'), { withFileTypes: true }))
    .filter(file => file.isFile() && file.name.endsWith('.md')).map(file => file.name).sort();
  assert.ok(references.length > 0, '安装的 Skill 必须带有可读取的 references');
  for (const file of references) assert.ok((await readFile(join(directory, 'references', file), 'utf8')).length > 0);
  // 只核包内导航目标，不冻结标题、正文或 reference 数量。
  const links = [...entry.matchAll(/\[[^\]]*\]\((references\/[^)#?]+\.md)(?:#[^)]*)?\)/gu)].map(match => match[1]);
  assert.ok(links.length > 0, 'Skill 入口必须能够定位安装的 references');
  for (const link of new Set(links)) {
    const target = await realpath(resolve(directory, link));
    assert.ok(within(directory, target), 'reference 入口必须位于同一 Skill 内');
    assert.ok((await readFile(target, 'utf8')).length > 0);
  }
  for (const [name, peerVersion] of Object.entries(installed.peerDependencies)) {
    assert.equal(peerVersion, installed.version);
    assert.equal(installed.peerDependenciesMeta[name].optional, true);
  }

  const commandName = 'dsh-reactive-skill', declaredBin = installed.bin?.[commandName];
  assert.ok(typeof declaredBin === 'string' && declaredBin, '安装包必须声明 Skill CLI');
  const binTarget = await realpath(resolve(packageRoot, declaredBin));
  assert.ok(within(packageRoot, binTarget));
  const commandPath = join(root, 'node_modules/.bin', commandName);
  assert.equal(await realpath(commandPath), binTarget, '实际 npm bin 必须指向 manifest 声明的 CLI');
  const command = await execute(commandPath, [], { cwd: root });
  assert.deepEqual(JSON.parse(command.stdout), manifest);
  return { ...manifest, references };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  console.log(JSON.stringify({ developerSkill: await checkDeveloperSkill(process.argv[2]) }));
}
