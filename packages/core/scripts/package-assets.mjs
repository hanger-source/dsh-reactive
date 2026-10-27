import { cp, lstat, mkdir, rename, stat } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../../../', import.meta.url));
const packageRoot = fileURLToPath(new URL('../', import.meta.url));
const assets = ['README.md', 'CONTRIBUTING.md', 'docs', 'examples/merchant-operations', 'examples/capabilities', 'examples/fleet-inspection'];
const saved = join(root, '.trash', 'core-package-assets-' + randomUUID());

// 仓库文档与正式 reference 是唯一源；包内目录只是本次构建的发行资产。
for (const asset of assets) await stat(join(root, asset));
for (const asset of assets) {
  const destination = join(packageRoot, asset);
  let previous = false;
  try { await lstat(destination); previous = true; }
  catch (error) { if (error.code !== 'ENOENT') throw error; }
  if (previous) {
    const archive = join(saved, asset);
    await mkdir(dirname(archive), { recursive: true });
    await rename(destination, archive);
  }
  await mkdir(dirname(destination), { recursive: true });
  await cp(join(root, asset), destination, { recursive: true });
}
