import { execFile, spawn } from 'node:child_process';
import { promisify } from 'node:util';
import { readFile, writeFile, realpath, lstat } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, resolve, relative, isAbsolute, join } from 'node:path';
import { createHash } from 'node:crypto';
import semver from 'semver';
import { getReleasePlan } from '@changesets/get-release-plan';

const execute = promisify(execFile);
export const root = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
export const json = async path => JSON.parse(await readFile(path, 'utf8'));
export const save = (path, value) => writeFile(path, JSON.stringify(value, null, 2) + '\n');
export const digest = async (path, algorithm = 'sha512') => createHash(algorithm).update(await readFile(path)).digest('base64');
export const configuration = () => json(join(root, 'scripts/release.config.json'));

export async function capture(command, args, cwd = root) {
  return (await execute(command, args, { cwd, maxBuffer: 64 * 1024 * 1024, env: process.env })).stdout.trim();
}
export async function run(command, args, cwd = root, env = process.env) {
  console.log('$', command, ...args);
  const process = spawn(command, args, { cwd, stdio: 'inherit', env });
  await new Promise((done, reject) => {
    process.once('error', reject);
    process.once('exit', (code, signal) => code === 0 ? done() : reject(new Error(`RELEASE_COMMAND_FAILED: ${command} exit=${code} signal=${signal}`)));
  });
}
export const npm = args => run('npm', args);
export const changeset = args => run(process.execPath, [join(root, 'node_modules/@changesets/cli/bin.js'), ...args]);

export function flags(args) {
  const selected = {};
  for (let index = 0; index < args.length; index++) {
    const flag = args[index];
    if (!/^--[a-z][a-z-]*$/u.test(flag)) throw new Error('RELEASE_OPTION_INVALID: ' + flag);
    const key = flag.slice(2);
    if (Object.hasOwn(selected, key)) throw new Error('RELEASE_OPTION_DUPLICATE: ' + key);
    if (key === 'also-latest') selected[key] = true;
    else {
      const value = args[++index];
      if (!value || value.startsWith('--')) throw new Error('RELEASE_OPTION_VALUE_REQUIRED: ' + key);
      selected[key] = value;
    }
  }
  return selected;
}
export function only(options, allowed) {
  for (const key of Object.keys(options)) if (!allowed.includes(key)) throw new Error('RELEASE_OPTION_UNKNOWN: ' + key);
}
export function channel(options) {
  if (options.channel !== 'next' && options.channel !== 'stable') throw new Error('RELEASE_CHANNEL_REQUIRED: next|stable');
  return options.channel;
}
export function registryUrl(value) {
  const url = new URL(value);
  if (url.protocol !== 'https:' || url.username || url.password || url.search || url.hash) throw new Error('RELEASE_REGISTRY_INVALID');
  return url.href.replace(/\/$/u, '');
}
export async function clean() {
  if (await capture('git', ['status', '--porcelain=v1', '--untracked-files=all'])) throw new Error('RELEASE_GIT_WORKTREE_NOT_CLEAN');
}
export async function head(expected) {
  const current = await capture('git', ['rev-parse', 'HEAD']);
  if (!/^[0-9a-f]{40}$/u.test(expected ?? '') || current !== expected) throw new Error('RELEASE_EXACT_HEAD_REQUIRED: ' + current);
  return current;
}
export async function tagCommit(tag, commit, required) {
  let kind;
  try { kind = await capture('git', ['cat-file', '-t', 'refs/tags/' + tag]); }
  catch (error) { if (!required && error.code === 128) return; throw error; }
  if (kind !== 'tag') throw new Error('RELEASE_ANNOTATED_TAG_REQUIRED: ' + tag);
  if (await capture('git', ['rev-parse', tag + '^{commit}']) !== commit) throw new Error('RELEASE_TAG_COMMIT_MISMATCH');
}
export function assertVersion(version, selected) {
  if (selected !== 'next' && selected !== 'stable') throw new Error('RELEASE_CHANNEL_REQUIRED: next|stable');
  if (!semver.valid(version)) throw new Error('RELEASE_SEMVER_INVALID: ' + version);
  const pre = semver.prerelease(version);
  if (selected === 'next' && (!pre || pre.length !== 2 || pre[0] !== 'next' || typeof pre[1] !== 'number')) throw new Error('RELEASE_NEXT_VERSION_REQUIRED');
  if (selected === 'stable' && pre) throw new Error('RELEASE_STABLE_VERSION_REQUIRED');
}
export async function packages(config, selected) {
  if ((await json(join(root, 'package.json'))).private !== true) throw new Error('RELEASE_PRIVATE_WORKSPACE_ROOT_REQUIRED');
  const entries = await Promise.all(config.packages.map(async item => ({ ...item, manifest: await json(join(root, item.directory, 'package.json')) })));
  const version = entries[0].manifest.version;
  if (selected) assertVersion(version, selected);
  const names = new Set(entries.map(item => item.name));
  if (names.size !== 6) throw new Error('RELEASE_SIX_PACKAGES_REQUIRED');
  for (const item of entries) {
    const pkg = item.manifest;
    if (pkg.name !== item.name || pkg.private || pkg.version !== version) throw new Error('RELEASE_PACKAGE_VERSION_MISMATCH: ' + item.name);
    for (const field of ['dependencies', 'optionalDependencies', 'peerDependencies']) {
      for (const [name, spec] of Object.entries(pkg[field] ?? {})) if (names.has(name) && spec !== version) throw new Error(`RELEASE_INTERNAL_DEPENDENCY_MISMATCH: ${item.name} ${field}.${name}=${spec}`);
    }
  }
  return { entries, version, versions: Object.fromEntries(entries.map(item => [item.name, version])) };
}
export async function releaseSource(options, requireTag = false) {
  const config = await configuration();
  const selected = channel(options), registry = registryUrl(options.registry ?? config.registry);
  await clean(); const commit = await head(options.commit);
  const data = await packages(config, selected);
  const tag = config.tagPrefix + data.version;
  if (options.tag !== tag) throw new Error('RELEASE_EXACT_TAG_REQUIRED: ' + tag);
  const plan = await getReleasePlan(root);
  const pending = plan.releases.filter(release => release.type !== 'none');
  if (pending.length) throw new Error('RELEASE_VERSIONING_REQUIRED: ' + pending.map(release => release.name + '@' + release.newVersion).join(', '));
  if (selected === 'next' ? plan.preState?.mode !== 'pre' || plan.preState.tag !== 'next' : plan.preState !== undefined)
    throw new Error('RELEASE_VERSIONING_CHANNEL_MISMATCH');
  await tagCommit(tag, commit, requireTag);
  for (const item of data.entries) {
    const pkg = item.manifest;
    if (registryUrl(pkg.publishConfig?.registry ?? '') !== registry) throw new Error('RELEASE_PACKAGE_REGISTRY_MISMATCH: ' + item.name);
    if (pkg.publishConfig.access !== 'public') throw new Error('RELEASE_PUBLIC_ACCESS_REQUIRED: ' + item.name);
    if (pkg.author !== config.author) throw new Error('RELEASE_PACKAGE_AUTHOR_MISMATCH: ' + item.name);
    if (pkg.publishConfig.tag !== 'next') throw new Error('RELEASE_SAFE_DEFAULT_TAG_REQUIRED: ' + item.name);
    if (pkg.repository?.url !== config.repository || pkg.repository.directory !== item.directory) throw new Error('RELEASE_PACKAGE_REPOSITORY_MISMATCH: ' + item.name);
    if (!pkg.files?.includes('release.json')) throw new Error('RELEASE_RECEIPT_NOT_PACKAGED: ' + item.name);
    const changelog = await readFile(join(root, item.directory, 'CHANGELOG.md'), 'utf8');
    if (changelog.split('\n').find(line => line.startsWith('## ')) !== '## ' + data.version)
      throw new Error('RELEASE_CHANGELOG_VERSION_REQUIRED: ' + item.name);
  }
  return { config, ...data, channel: selected, registry, commit, tag, lockSha256: await digest(join(root, 'package-lock.json'), 'sha256') };
}
export function beneath(parent, child) {
  const path = relative(parent, child);
  return path === '' || (!path.startsWith('..' + (process.platform === 'win32' ? '\\' : '/')) && path !== '..' && !isAbsolute(path));
}
export async function independent(path) {
  if ((await lstat(path)).isSymbolicLink()) throw new Error('RELEASE_CONSUMER_SYMLINK_REFUSED');
  const actual = await realpath(path), workspace = await realpath(root);
  if (beneath(workspace, actual)) throw new Error('RELEASE_CONSUMER_MUST_BE_OUTSIDE_WORKSPACE');
  return actual;
}
export function node24() {
  if (!semver.satisfies(process.versions.node, '>=24.18.0 <25')) throw new Error('RELEASE_NODE_24_18_REQUIRED');
}
