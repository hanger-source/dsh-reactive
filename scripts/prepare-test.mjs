import { lstat, mkdir, rename, stat, symlink } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { dirname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
const mode = process.argv[2];
if (mode !== 'reset' && mode !== 'link') throw new Error('REACTIVE_TEST_PREPARATION_MODE_REQUIRED');
const saved = join(root, '.trash', 'test-build-' + randomUUID());
if (mode === 'reset') {
  const output = join(root, '.tmp/test');
  let previous = false;
  try { await lstat(output); previous = true; }
  catch (error) { if (error.code !== 'ENOENT') throw error; }
  // 编译不能穿过上轮的源码映射链接写回正式产物。
  if (previous) {
    await mkdir(saved, { recursive: true });
    await rename(output, join(saved, 'output'));
  }
} else {
  const mappings = [
    ['.tmp/test/packages/core/src', 'packages/core/lib', 'core'],
    ['.tmp/test/packages/dsh/src', 'packages/dsh/lib', 'dsh'],
  ];
  for (const [, target] of mappings) {
    if (!(await stat(join(root, target))).isDirectory()) throw new Error('REACTIVE_TEST_ARTIFACT_DIRECTORY_REQUIRED: ' + target);
  }
  for (const [source, target, name] of mappings) {
    const link = join(root, source);
    try {
      await lstat(link);
      await mkdir(saved, { recursive: true });
      await rename(link, join(saved, name));
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
    }
    await mkdir(dirname(link), { recursive: true });
    await symlink(relative(dirname(link), join(root, target)), link, 'dir');
  }
}
