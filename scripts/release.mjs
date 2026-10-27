#!/usr/bin/env node
import { flags, only, configuration, clean, head, capture, run, releaseSource, node24 } from './release/common.mjs';
import { prepare } from './release/artifacts.mjs';
import { plan, version } from './release/version.mjs';
import { verify, publish } from './release/registry.mjs';

const [command, ...args] = process.argv.slice(2);
const options = flags(args);
const usage = `发行命令（不会自动 commit 或 push）：
  npm run changeset -- --minor @hanger-source/dsh-reactive --message "变更说明"
  npm run release -- plan [--out /absolute/plan.json]
  npm run release -- version --channel next|stable
  npm run release -- prepare --channel next|stable --commit FULL_SHA --tag dsh-reactive@VERSION [--out DIRECTORY]
  npm run release -- verify-artifacts --manifest DIRECTORY/release-manifest.json
  npm run release -- tag --channel next|stable --commit FULL_SHA --tag dsh-reactive@VERSION
  npm run release -- publish --manifest DIRECTORY/release-manifest.json [--also-latest]
  npm run release -- verify --manifest DIRECTORY/release-manifest.json

next 只允许 -next.N。stable 只允许正式版本，默认只写 stable；另给 --also-latest 才更新 latest。
verify 从配置的 registry 安装；verify-artifacts 仅是发布前候选包验证，不代表已发布。
`;

try {
  if (!command || command === 'help' || command === '--help') console.log(usage);
  else {
    node24();
    if (command === 'plan') { only(options, ['out']); await plan(options); }
    else if (command === 'version') { only(options, ['channel']); await version(options); }
    else if (command === 'prepare') {
      only(options, ['channel', 'commit', 'tag', 'out', 'registry']); await prepare(options);
    } else if (command === 'tag') {
      only(options, ['channel', 'commit', 'tag']);
      const source = await releaseSource(options);
      // 已有 tag 由 releaseSource 核 exact commit；同次命令可安全重读，不覆盖任何 tag。
      const existing = await capture('git', ['tag', '--list', source.tag]);
      if (!existing) await run('git', ['tag', '-a', source.tag, source.commit, '-m', `六包发行 ${source.version}\ncommit ${source.commit}\nchannel ${source.channel}`]);
      console.log(JSON.stringify({ tag: source.tag, commit: source.commit, pushed: false }));
    } else if (command === 'verify' || command === 'verify-artifacts') {
      only(options, ['manifest']); if (!options.manifest) throw new Error('RELEASE_MANIFEST_REQUIRED');
      await verify(options.manifest, command === 'verify' ? 'registry' : 'artifacts');
    } else if (command === 'publish') {
      only(options, ['manifest', 'also-latest']); if (!options.manifest) throw new Error('RELEASE_MANIFEST_REQUIRED');
      await publish(options);
    } else throw new Error('RELEASE_COMMAND_UNKNOWN: ' + command);
  }
} catch (error) { console.error(error); process.exitCode = 1; }
