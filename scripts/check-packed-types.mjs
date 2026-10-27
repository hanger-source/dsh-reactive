import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { access, copyFile, lstat, mkdir, mkdtemp, readFile, realpath, writeFile } from 'node:fs/promises';
import { basename, dirname, join, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
const config = JSON.parse(await readFile(join(root, 'scripts/release.config.json'), 'utf8'));
assert.equal(config.registry, 'https://registry.npmjs.org');
if (!process.argv[2]) throw new Error('REACTIVE_PACK_REPORT_REQUIRED');
const report = resolve(process.argv[2]);
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
}

async function run(command, args, cwd) {
  const child = spawn(command, args, { cwd, stdio: 'inherit' });
  await new Promise((done, reject) => {
    child.once('error', reject);
    child.once('exit', (code, signal) => code === 0 ? done() : reject(new Error(`REACTIVE_PACK_CONSUMER_FAILED: ${command} exit=${code} signal=${signal}`)));
  });
}
const location = await realpath(await mkdtemp(join(tmpdir(), 'dsh-reactive-packed-types-')));
for (const kind of ['backend', 'ui']) {
  const directory = join(location, kind);
  await mkdir(directory);
  const names = kind === 'backend'
    ? config.packages.filter(item => item.name !== '@hanger-source/dsh-reactive-native-ui').map(item => item.name)
    : ['@hanger-source/dsh-reactive', '@hanger-source/dsh-reactive-client', '@hanger-source/dsh-reactive-native-ui', '@hanger-source/dsh-reactive-skills'];
  const dependencies = Object.fromEntries(names.map(name => [name, join(dirname(report), packed.get(name).filename)]));
  dependencies.react = kind === 'backend' ? config.consumerTooling.backendReact : config.consumerTooling.uiReact;
  if (kind === 'backend') dependencies['react-reconciler'] = config.consumerTooling.backendReconciler;
  await writeFile(join(directory, 'package.json'), JSON.stringify({ private: true, type: 'module', dependencies,
    devDependencies: { typescript: config.consumerTooling.typescript, '@types/node': config.consumerTooling.nodeTypes,
      '@types/react': kind === 'backend' ? config.consumerTooling.backendReactTypes : config.consumerTooling.uiReactTypes } }, null, 2));
  await writeFile(join(directory, '.npmrc'), 'registry=https://registry.npmjs.org\n@hanger-source:registry=https://registry.npmjs.org\n');
  await run('npm', ['install', '--ignore-scripts', '--no-audit', '--no-fund', '--registry', config.registry], directory);
  for (const name of names) {
    const installed = join(directory, 'node_modules', name);
    assert.equal((await lstat(installed)).isSymbolicLink(), false, '必须消费实际 tgz，不能链接工作区');
    assert.equal(await realpath(installed), installed);
    assert.equal(JSON.parse(await readFile(join(installed, 'package.json'), 'utf8')).version, packed.get(name).version);
  }
  await copyFile(join(root, `scripts/release/consumer-${kind}.ts`), join(directory, 'consumer.ts'));
  await writeFile(join(directory, 'tsconfig.json'), JSON.stringify({ compilerOptions: { target: 'ES2024', module: 'NodeNext',
    moduleResolution: 'NodeNext', strict: true, skipLibCheck: false, noEmit: true, types: ['node'], jsx: 'react-jsx' }, include: ['consumer.ts'] }, null, 2));
  await run(process.execPath, [join(directory, 'node_modules/typescript/bin/tsc'), '-p', 'tsconfig.json'], directory);
  console.log(JSON.stringify({ consumer: kind, directory, packages: names, strict: true, source: 'workspace-tarballs' }));
}
