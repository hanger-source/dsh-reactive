import { glob, lstat, mkdir, readFile, rename } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
const directory = process.cwd();
const manifest = JSON.parse(await readFile(join(directory, 'package.json'), 'utf8'));
const output = manifest.wireit?.[process.argv[2]]?.output;
if (!Array.isArray(output) || !output.length) throw new Error('REACTIVE_BUILD_OUTPUT_REQUIRED');
const archive = join(root, '.trash', 'build-output-' + randomUUID());
// 输出清单就是任务的唯一所有权声明；保留其他任务的合同、生成贡献与浏览器产物。
const patterns = output.filter(pattern => !pattern.startsWith('!'));
const exclude = output.filter(pattern => pattern.startsWith('!')).map(pattern => pattern.slice(1));
for await (const path of glob(patterns, { cwd: directory, exclude })) {
  const source = resolve(directory, path);
  const local = relative(directory, source);
  if (local.startsWith('..') || local === '') throw new Error('REACTIVE_BUILD_OUTPUT_OUTSIDE_PACKAGE');
  if ((await lstat(source)).isDirectory()) continue;
  const saved = join(archive, relative(root, source));
  await mkdir(dirname(saved), { recursive: true });
  await rename(source, saved);
}
