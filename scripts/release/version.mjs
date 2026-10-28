import { copyFile, mkdir, mkdtemp } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { getReleasePlan } from '@changesets/get-release-plan';
import { changeset, channel, clean, configuration, json, packages, root, run, save } from './common.mjs';

export async function version(options) {
  const selected = channel(options), config = await configuration();
  await clean(); await packages(config);
  let pre;
  try { pre = await json(join(root, '.changeset/pre.json')); }
  catch (error) { if (error.code !== 'ENOENT') throw error; }
  if (selected === 'next' && (pre?.mode === 'exit' || pre?.mode === 'pre' && pre.tag !== 'next'))
    throw new Error('RELEASE_PRERELEASE_MODE_MISMATCH');
  let archive;
  const preserve = async file => {
    if (!archive) {
      await mkdir(join(root, '.trash'), { recursive: true });
      archive = await mkdtemp(join(root, '.trash', 'release-version-'));
    }
    const target = join(archive, '.changeset', file);
    await mkdir(dirname(target), { recursive: true });
    await copyFile(join(root, '.changeset', file), target);
  };
  if (pre) await preserve('pre.json');
  const pending = await getReleasePlan(root);
  for (const item of pending.changesets) await preserve(item.id + '.md');
  if (selected === 'next') {
    if (!pre) await changeset(['pre', 'enter', 'next']);
  } else if (pre?.mode === 'pre') {
    await changeset(['pre', 'exit']);
    const exiting = await getReleasePlan(root);
    for (const item of exiting.changesets) await preserve(item.id + '.md');
  }
  await changeset(['version']);
  await run('npm', ['install', '--package-lock-only', '--ignore-scripts', '--no-audit', '--no-fund', '--registry', config.registry]);
  const result = await packages(config, selected);
  console.log(JSON.stringify({ version: result.version, channel: selected, changesetsArchive: archive,
    review: '检查版本、CHANGELOG、内部依赖与锁，然后显式提交。尚未打 tag 或发布。' }, null, 2));
}

export async function plan(options) {
  const out = resolve(options.out ?? join(root, '.tmp/release-plan.json'));
  await mkdir(resolve(out, '..'), { recursive: true });
  const result = await getReleasePlan(root);
  await save(out, result);
  console.log(JSON.stringify(result, null, 2));
}
