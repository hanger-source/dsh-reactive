import { readFile } from 'node:fs/promises';
import { publint } from 'publint';
import { formatMessage } from 'publint/utils';
import { capture } from './common.mjs';

async function check(options) {
  const { messages, pkg } = await publint(options);
  for (const message of messages) console.log(formatMessage(message, pkg));
  if (messages.some(message => message.type === 'error')) throw new Error('RELEASE_PACKAGE_LINT_FAILED: ' + pkg.name);
}

export const lintPackage = pkgDir => check({ pkgDir, pack: false });
export async function lintTarball(path) {
  const files = (await capture('tar', ['-tzf', path])).split('\n');
  if (!files.includes('package/CHANGELOG.md')) throw new Error('RELEASE_PACKAGE_CHANGELOG_MISSING:' + path);
  const buffer = await readFile(path);
  await check({ pack: { tarball: buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength) } });
}
