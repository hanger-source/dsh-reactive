import { mkdir, readFile } from 'node:fs/promises';
import { join, resolve, dirname, basename } from 'node:path';
import { capture, run, root, json, save, digest, releaseSource, configuration, registryUrl, assertVersion } from './common.mjs';
import { lintTarball } from './lint.mjs';

async function unchanged(source) {
  const current = await releaseSource({ commit: source.commit, tag: source.tag, channel: source.channel, registry: source.registry });
  if (current.lockSha256 !== source.lockSha256) throw new Error('RELEASE_SOURCE_LOCK_CHANGED');
}

export async function prepare(options) {
  const source = await releaseSource(options);
  const out = resolve(options.out ?? join(root, '.tmp/releases', source.version));
  await mkdir(dirname(out), { recursive: true });
  await mkdir(out); // 已有现场不覆盖，重复准备必须选新的明确目录。
  await run('npm', ['run', 'prepare:packages']);
  await unchanged(source);
  const raw = join(out, 'raw'), staging = join(out, 'staging'), ready = join(out, 'packages');
  await Promise.all([raw, staging, ready].map(path => mkdir(path)));
  const artifacts = [];
  for (const entry of source.entries) {
    const packed = JSON.parse(await capture('npm', ['pack', resolve(root, entry.directory), '--json', '--ignore-scripts', '--pack-destination', raw]));
    if (packed.length !== 1 || packed[0].name !== entry.name || packed[0].version !== source.version) throw new Error('RELEASE_PACK_IDENTITY_MISMATCH');
    const stage = join(staging, entry.name.replaceAll('/', '-'));
    await mkdir(stage);
    await run('tar', ['-xzf', join(raw, packed[0].filename), '-C', stage]);
    const directory = join(stage, 'package');
    const manifest = await json(join(directory, 'package.json'));
    manifest.gitHead = source.commit;
    manifest.publishConfig = { ...manifest.publishConfig, registry: source.registry, tag: source.channel };
    await save(join(directory, 'package.json'), manifest);
    const receipt = { package: entry.name, version: source.version, commit: source.commit, tag: source.tag,
      registry: source.registry, channel: source.channel, versions: source.versions, lockSha256: source.lockSha256 };
    await save(join(directory, 'release.json'), receipt);
    const final = JSON.parse(await capture('npm', ['pack', directory, '--json', '--ignore-scripts', '--pack-destination', ready]));
    if (final.length !== 1 || final[0].name !== entry.name || final[0].version !== source.version
      || !final[0].files.some(file => file.path === 'release.json')) throw new Error('RELEASE_FINAL_PACK_INVALID');
    const file = 'packages/' + final[0].filename;
    // 直接检查最终 tgz，避免 publint 再 pack 或触发任何 prepack/build。
    await lintTarball(join(out, file));
    const integrity = 'sha512-' + await digest(join(out, file));
    if (integrity !== final[0].integrity) throw new Error('RELEASE_PACK_INTEGRITY_MISMATCH');
    artifacts.push({ name: entry.name, version: source.version, directory: entry.directory, file, integrity, dependencies: {
      dependencies: manifest.dependencies ?? {}, optionalDependencies: manifest.optionalDependencies ?? {}, peerDependencies: manifest.peerDependencies ?? {},
    } });
  }
  const manifest = { commit: source.commit, tag: source.tag, version: source.version, channel: source.channel,
    registry: source.registry, lockSha256: source.lockSha256, artifacts };
  await unchanged(source);
  await save(join(out, 'release-manifest.json'), manifest);
  console.log(JSON.stringify({ prepared: join(out, 'release-manifest.json'), commit: source.commit, version: source.version, published: false }, null, 2));
}

export async function artifactManifest(path) {
  const file = resolve(path), directory = dirname(file), manifest = await json(file);
  const config = await configuration();
  assertVersion(manifest.version, manifest.channel);
  if (registryUrl(manifest.registry) !== registryUrl(config.registry)) throw new Error('RELEASE_MANIFEST_REGISTRY_MISMATCH');
  if (!/^[0-9a-f]{40}$/u.test(manifest.commit ?? '') || manifest.tag !== config.tagPrefix + manifest.version) throw new Error('RELEASE_MANIFEST_SOURCE_INVALID');
  const expected = new Map(config.packages.map(item => [item.name, item.directory]));
  if (!Array.isArray(manifest.artifacts) || manifest.artifacts.length !== expected.size) throw new Error('RELEASE_MANIFEST_PACKAGE_COUNT_MISMATCH');
  const names = new Set();
  for (const item of manifest.artifacts) {
    if (names.has(item.name) || expected.get(item.name) !== item.directory || item.version !== manifest.version
      || typeof item.file !== 'string' || item.file !== 'packages/' + basename(item.file)) throw new Error('RELEASE_MANIFEST_PACKAGE_INVALID');
    names.add(item.name);
    if (item.integrity !== 'sha512-' + await digest(join(directory, item.file))) throw new Error('RELEASE_ARTIFACT_CHANGED: ' + item.name);
  }
  return { file, directory, manifest, manifestSha256: await digest(file, 'sha256') };
}

export async function currentArtifacts(path, requireTag) {
  const artifact = await artifactManifest(path);
  const source = await releaseSource({ commit: artifact.manifest.commit, tag: artifact.manifest.tag,
    channel: artifact.manifest.channel, registry: artifact.manifest.registry }, requireTag);
  if (source.version !== artifact.manifest.version || source.lockSha256 !== artifact.manifest.lockSha256) throw new Error('RELEASE_SOURCE_ARTIFACT_MISMATCH');
  for (const item of artifact.manifest.artifacts) {
    const current = source.entries.find(entry => entry.name === item.name).manifest;
    for (const field of ['dependencies', 'optionalDependencies', 'peerDependencies']) {
      if (JSON.stringify(current[field] ?? {}) !== JSON.stringify(item.dependencies[field])) throw new Error('RELEASE_ARTIFACT_DEPENDENCY_MISMATCH: ' + item.name);
    }
  }
  return { ...artifact, source };
}
