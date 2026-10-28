import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { access, lstat, mkdir, mkdtemp, readFile, realpath, writeFile } from 'node:fs/promises';
import { basename, dirname, join, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';
import { consumerKinds, consumerPackages, consumerEnvironment, prepareConsumerSources, checkConsumerDependencies } from './release/consumer-profiles.mjs';
import { buildConsumerBrowser, buildNativeContractsBrowser } from './release/consumer-browser.mjs';
import { lintTarball } from './release/lint.mjs';

const root = fileURLToPath(new URL('../', import.meta.url));
const config = JSON.parse(await readFile(join(root, 'scripts/release.config.json'), 'utf8'));
assert.equal(config.registry, 'https://registry.npmjs.org');
if (!process.argv[2]) throw new Error('REACTIVE_PACK_REPORT_REQUIRED');
const report = resolve(process.argv[2]);
const selected = process.argv.slice(3).map(argument => {
  const kind = argument.startsWith('--consumer=') ? argument.slice('--consumer='.length) : undefined;
  if (!consumerKinds.includes(kind)) throw new Error('REACTIVE_PACK_CONSUMER_INVALID: ' + argument);
  return kind;
});
const kinds = selected.length ? [...new Set(selected)] : consumerKinds;
const artifacts = JSON.parse(await readFile(report, 'utf8'));
assert.equal(artifacts.length, config.packages.length);
const packed = new Map(artifacts.map(item => [item.name, item]));
assert.equal(packed.size, config.packages.length);
for (const entry of config.packages) {
  const manifest = JSON.parse(await readFile(join(root, entry.directory, 'package.json'), 'utf8'));
  const artifact = packed.get(entry.name);
  assert.equal(artifact?.name, manifest.name);
  assert.equal(artifact.version, manifest.version);
  assert.equal(artifact.filename, basename(artifact.filename));
  await access(join(dirname(report), artifact.filename));
  await lintTarball(join(dirname(report), artifact.filename));
}

async function run(command, args, cwd) {
  const child = spawn(command, args, { cwd, stdio: 'inherit' });
  await new Promise((done, reject) => {
    child.once('error', reject);
    child.once('exit', (code, signal) => code === 0 ? done() : reject(new Error(`REACTIVE_PACK_CONSUMER_FAILED: ${command} exit=${code} signal=${signal}`)));
  });
}
const location = await realpath(await mkdtemp(join(tmpdir(), 'dsh-reactive-packed-types-')));
const tarballs = { host: join(dirname(report), packed.get('@hanger-source/dsh-reactive-dsh').filename),
  core: join(dirname(report), packed.get('@hanger-source/dsh-reactive').filename),
  nativeUi: join(dirname(report), packed.get('@hanger-source/dsh-reactive-native-ui').filename) };
for (const kind of kinds) {
  const directory = join(location, kind);
  await mkdir(directory);
  console.log(JSON.stringify({ consumer: kind, directory, phase: 'installing' }));
  const names = consumerPackages(config, kind);
  const dependencies = Object.fromEntries(names.map(name => [name, join(dirname(report), packed.get(name).filename)]));
  const environment = await consumerEnvironment(config, kind, tarballs);
  await writeFile(join(directory, 'package.json'), JSON.stringify({ private: true, type: 'module',
    dependencies: { ...dependencies, ...environment.dependencies }, devDependencies: environment.devDependencies }, null, 2));
  await writeFile(join(directory, '.npmrc'), 'registry=https://registry.npmjs.org\n@hanger-source:registry=https://registry.npmjs.org\n');
  await run('npm', ['install', '--ignore-scripts', '--no-audit', '--no-fund', '--registry', config.registry], directory);
  for (const name of names) {
    const installed = join(directory, 'node_modules', name);
    assert.equal((await lstat(installed)).isSymbolicLink(), false, '必须消费实际 tgz，不能链接工作区');
    assert.equal(await realpath(installed), installed);
    assert.equal(JSON.parse(await readFile(join(installed, 'package.json'), 'utf8')).version, packed.get(name).version);
  }
  await prepareConsumerSources(directory, kind);
  await run(process.execPath, [join(directory, 'node_modules/typescript/bin/tsc'), '-p', 'tsconfig.json'], directory);
  await checkConsumerDependencies(directory, kind);
  // 执行同一份仓外消费者：Program 的根合同必须被 /program 接受，UI 消费不加载后台 renderer。
  await run(process.execPath, [join(directory, 'compiled/consumer.js')], directory);
  await run(process.execPath, [join(directory, 'runtime.mjs'), '--client-entries'], directory);
  await run(process.execPath, [join(directory, 'consumer-skill.mjs'), packed.get('@hanger-source/dsh-reactive-skills').version], directory);
  const browser = kind === 'ui' ? await buildConsumerBrowser(directory) : undefined;
  const nativeContractsBrowser = kind === 'native' ? await buildNativeContractsBrowser(directory) : undefined;
  console.log(JSON.stringify({ consumer: kind, directory, packages: names, strict: true, declaredDependencies: true, runtime: true, browser, nativeContractsBrowser, source: 'workspace-tarballs' }));
}
