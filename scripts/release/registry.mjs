import { mkdtemp, mkdir, copyFile, realpath, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { capture, run, root, configuration, save, json, independent, assertVersion, clean } from './common.mjs';
import { artifactManifest, currentArtifacts } from './artifacts.mjs';
import { setTimeout as delay } from 'node:timers/promises';

async function metadata(name, version, registry) {
  try {
    return JSON.parse(await capture('npm', ['view', name + '@' + version, 'name', 'version', 'gitHead', 'dist', 'dist-tags', '--json', '--prefer-online', '--registry', registry]));
  } catch (error) {
    let code;
    try { code = JSON.parse(error.stdout).error?.code; } catch {}
    // E404 仅说明当前 registry 对该版本不可见，不据此声称已有发布权限。
    if (code === 'E404') return undefined;
    throw error;
  }
}
function checkMetadata(value, item, manifest) {
  if (!value || value.name !== item.name || value.version !== item.version || value.dist?.integrity !== item.integrity) throw new Error('RELEASE_REGISTRY_ARTIFACT_MISMATCH: ' + item.name);
  if (value.gitHead !== undefined && value.gitHead !== manifest.commit) throw new Error('RELEASE_REGISTRY_COMMIT_MISMATCH: ' + item.name);
}
export async function publishedMetadata(item, manifest) {
  const deadline = Date.now() + 15 * 60_000;
  let announced = false;
  for (;;) {
    const value = await metadata(item.name, item.version, manifest.registry);
    if (value) {
      checkMetadata(value, item, manifest);
      return value;
    }
    if (Date.now() >= deadline) throw new Error('RELEASE_REGISTRY_VERSION_NOT_VISIBLE: ' + item.name + '@' + item.version);
    if (!announced) {
      console.log('等待 npm 已受理版本在 registry 可见：' + item.name + '@' + item.version);
      announced = true;
    }
    await delay(5000);
  }
}
async function distTags(name, registry, allowMissing = false) {
  try { return JSON.parse(await capture('npm', ['view', name, 'dist-tags', '--json', '--prefer-online', '--registry', registry])); }
  catch (error) {
    let code;
    try { code = JSON.parse(error.stdout).error?.code; } catch {}
    if (allowMissing && code === 'E404') return {};
    throw error;
  }
}
function checkTags(tags, manifest) {
  if (tags[manifest.channel] !== manifest.version) throw new Error('RELEASE_REGISTRY_CHANNEL_MISMATCH');
  if (manifest.channel === 'next' && (tags.latest === manifest.version || tags.stable === manifest.version)) {
    throw new Error('RELEASE_PRERELEASE_STABLE_TAG: registry 仍将候选放入稳定 tag，未满足渠道约束。');
  }
}
export async function publishedLatest(item, manifest, previous) {
  const deadline = Date.now() + 15 * 60_000;
  let announced = false;
  for (;;) {
    const tags = await distTags(item.name, manifest.registry);
    if (tags.latest === manifest.version) return tags;
    if (tags.latest !== previous) throw new Error('RELEASE_UNEXPECTED_LATEST_TAG_CHANGE: ' + item.name);
    if (Date.now() >= deadline) throw new Error('RELEASE_LATEST_TAG_NOT_CONFIRMED: ' + item.name);
    if (!announced) {
      console.log('等待 npm 已受理的 latest 标签公开可见：' + item.name + '@' + manifest.version);
      announced = true;
    }
    await delay(5000);
  }
}

export async function verify(path, source) {
  const artifact = await artifactManifest(path), { manifest, directory } = artifact;
  const config = await configuration();
  assertVersion(manifest.version, manifest.channel);
  const registryRecords = new Map();
  if (source === 'registry') {
    for (const item of manifest.artifacts) {
      const actual = await metadata(item.name, item.version, manifest.registry);
      checkMetadata(actual, item, manifest); registryRecords.set(item.name, actual);
      checkTags(await distTags(item.name, manifest.registry), manifest);
    }
  }
  const location = await realpath(await mkdtemp(join(tmpdir(), 'dsh-reactive-' + source + '-consumer-')));
  const consumers = [];
  for (const kind of ['backend', 'ui']) {
    const names = kind === 'backend' ? config.packages.filter(item => item.name !== '@hanger-source/dsh-reactive-native-ui').map(item => item.name)
      : ['@hanger-source/dsh-reactive', '@hanger-source/dsh-reactive-client', '@hanger-source/dsh-reactive-native-ui', '@hanger-source/dsh-reactive-skills'];
    const cwd = join(location, kind); await mkdir(cwd); await independent(cwd);
    const dependencies = Object.fromEntries(names.map(name => [name, source === 'registry' ? manifest.version
      : resolve(directory, manifest.artifacts.find(item => item.name === name).file)]));
    dependencies.react = kind === 'backend' ? config.consumerTooling.backendReact : config.consumerTooling.uiReact;
    if (kind === 'backend') dependencies['react-reconciler'] = config.consumerTooling.backendReconciler;
    await save(join(cwd, 'package.json'), { name: 'dsh-release-' + kind, private: true, type: 'module', dependencies,
      devDependencies: { typescript: config.consumerTooling.typescript, '@types/node': config.consumerTooling.nodeTypes,
        '@types/react': kind === 'backend' ? config.consumerTooling.backendReactTypes : config.consumerTooling.uiReactTypes } });
    // 只固定 registry，不复制凭据。认证继续由用户正式 npm 配置提供。
    await writeFile(join(cwd, '.npmrc'), `registry=${manifest.registry}\n@hanger-source:registry=${manifest.registry}\n`);
    await run('npm', ['install', '--ignore-scripts', '--no-audit', '--no-fund', '--registry', manifest.registry], cwd);
    if (source === 'registry') {
      const lock = await json(join(cwd, 'package-lock.json'));
      for (const name of names) {
        const installed = lock.packages['node_modules/' + name];
        if (installed?.version !== manifest.version || installed.integrity !== registryRecords.get(name).dist.integrity
          || installed.resolved !== registryRecords.get(name).dist.tarball) throw new Error('RELEASE_INSTALL_NOT_FROM_REGISTRY: ' + name);
      }
    }
    await copyFile(join(root, 'scripts/release/consumer-' + kind + '.ts'), join(cwd, 'consumer.ts'));
    await copyFile(join(root, 'scripts/release/consumer-runtime.mjs'), join(cwd, 'runtime.mjs'));
    await save(join(cwd, 'expected.json'), { kind, source, packages: names, version: manifest.version,
      commit: manifest.commit, tag: manifest.tag, registry: manifest.registry });
    await save(join(cwd, 'tsconfig.json'), { compilerOptions: { target: 'ES2024', module: 'NodeNext', moduleResolution: 'NodeNext',
      strict: true, skipLibCheck: false, noEmit: true, types: ['node'], jsx: 'react-jsx' }, include: ['consumer.ts'] });
    await run(process.execPath, [join(cwd, 'node_modules/typescript/bin/tsc'), '-p', 'tsconfig.json'], cwd);
    await run(process.execPath, [join(cwd, 'runtime.mjs')], cwd);
    consumers.push({ kind, directory: cwd, packages: names, strict: true, runtimeImport: true });
  }
  const report = { source, commit: manifest.commit, version: manifest.version, registry: manifest.registry,
    manifestSha256: artifact.manifestSha256, consumers };
  await save(join(directory, source === 'registry' ? 'registry-verification.json' : 'candidate-verification.json'), report);
  console.log(JSON.stringify(report, null, 2));
  return report;
}

export async function publish(options) {
  const artifact = await currentArtifacts(options.manifest, true), { manifest } = artifact;
  const proof = await json(join(artifact.directory, 'candidate-verification.json'));
  if (proof.source !== 'artifacts' || proof.manifestSha256 !== artifact.manifestSha256 || proof.commit !== manifest.commit) throw new Error('RELEASE_VERIFIED_CANDIDATE_REQUIRED');
  if (options['also-latest'] && manifest.channel !== 'stable') throw new Error('RELEASE_LATEST_REQUIRES_STABLE');
  const stateFile = join(artifact.directory, 'publication.json');
  let state;
  try { state = await json(stateFile); }
  catch (error) { if (error.code !== 'ENOENT') throw error; }
  if (state && state.manifestSha256 !== artifact.manifestSha256) throw new Error('RELEASE_PUBLICATION_STATE_MISMATCH');
  if (!state) {
    const beforeTags = {};
    for (const item of manifest.artifacts) beforeTags[item.name] = await distTags(item.name, manifest.registry, true);
    state = { manifestSha256: artifact.manifestSha256, commit: manifest.commit, version: manifest.version,
      registry: manifest.registry, beforeTags, completed: false };
    await save(stateFile, state);
  }
  // 首先核所有已有版本。不可用/E404 不是写权证明；真正 npm publish 的响应才确认受理。
  const existing = new Set();
  for (const item of manifest.artifacts) {
    const actual = await metadata(item.name, item.version, manifest.registry);
    if (actual) {
      checkMetadata(actual, item, manifest);
      // 部分发行继续只接受同一不可变版本与渠道，不覆盖版本或回退 tag。
      const tags = await distTags(item.name, manifest.registry);
      if (tags[manifest.channel] !== manifest.version) throw new Error('RELEASE_EXISTING_VERSION_CHANNEL_MISMATCH: ' + item.name);
      existing.add(item.name);
    }
  }
  const confirm = async (item, accepted) => {
    const actual = accepted ? await publishedMetadata(item, manifest) : await metadata(item.name, item.version, manifest.registry);
    const after = await distTags(item.name, manifest.registry, true);
    const before = state.beforeTags[item.name];
    if (manifest.channel === 'next') {
      for (const tag of ['latest', 'stable']) {
        if (before[tag] !== after[tag]) throw new Error('RELEASE_UNEXPECTED_STABLE_TAG_CHANGE: ' + item.name + ':' + tag);
      }
    }
    checkMetadata(actual, item, manifest); checkTags(after, manifest);
    state[item.name] = { integrity: actual.dist.integrity, tags: after };
    await save(stateFile, state);
  };
  // 先提交整批缺失版本，再核对 registry；各包扫描和分发可同时进行。
  for (const item of manifest.artifacts) {
    if (existing.has(item.name)) continue;
    try {
      await run('npm', ['publish', join(artifact.directory, item.file), '--ignore-scripts', '--registry', manifest.registry,
        '--tag', manifest.channel, '--access', 'public']);
    } catch (publishError) {
      // 回执失败时立即核实际状态，不把认证或未知失败当作已受理的传播等待。
      try { await confirm(item, false); }
      catch (error) { throw new AggregateError([publishError, error], 'RELEASE_PUBLISH_AND_REGISTRY_CONFIRMATION_FAILED: ' + item.name); }
      throw publishError;
    }
  }
  for (const item of manifest.artifacts) await confirm(item, true);
  await verify(artifact.file, 'registry');
  if (options['also-latest']) {
    const previous = new Map();
    for (const item of manifest.artifacts) {
      previous.set(item.name, (await distTags(item.name, manifest.registry)).latest);
      await run('npm', ['dist-tag', 'add', item.name + '@' + item.version, 'latest', '--registry', manifest.registry]);
    }
    for (const item of manifest.artifacts) await publishedLatest(item, manifest, previous.get(item.name));
  }
  await clean();
  state.completed = true;
  state.latestRequested = Boolean(options['also-latest']);
  for (const item of manifest.artifacts) state[item.name].tags = await distTags(item.name, manifest.registry);
  await save(stateFile, state);
  console.log(JSON.stringify({ published: true, registryInstalled: true, channel: manifest.channel,
    version: manifest.version, commit: manifest.commit, tag: manifest.tag, latestRequested: Boolean(options['also-latest']), pushed: false }, null, 2));
}
