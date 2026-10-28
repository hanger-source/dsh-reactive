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
  const manifest = JSON.parse(await capture('tar', ['-xOf', path, 'package/package.json']));
  if (!Array.isArray(manifest.files) || manifest.files.some(entry => typeof entry !== 'string')) {
    throw new Error('RELEASE_PACKAGE_FILES_REQUIRED:' + path);
  }
  const packedFiles = files.filter(file => !file.endsWith('/'));
  for (const entry of manifest.files) {
    // glob不作字面路径判断；类型与运行入口由publint检查，release.json归正式来源核验。
    if (/[?*\[\]{}()!]/u.test(entry)) continue;
    const literal = entry.replace(/^(?:\.\/)+/u, '').replace(/\/+$/u, '');
    if (literal === 'release.json') continue;
    const expected = 'package/' + literal;
    if (!packedFiles.some(file => file === expected || file.startsWith(expected + '/'))) {
      throw new Error('RELEASE_PACKAGE_FILE_MISSING:' + manifest.name + ':' + entry);
    }
  }
  const buffer = await readFile(path);
  await check({ pack: { tarball: buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength) } });
}
