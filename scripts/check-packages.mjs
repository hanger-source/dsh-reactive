import { resolve } from 'node:path';
import { configuration, root } from './release/common.mjs';
import { lintPackage } from './release/lint.mjs';

for (const entry of (await configuration()).packages) {
  console.log('检查已构建的发行单元：', entry.name);
  await lintPackage(resolve(root, entry.directory));
}
