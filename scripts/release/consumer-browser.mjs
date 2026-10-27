import { readFile, realpath } from 'node:fs/promises';
import { isAbsolute, join, relative, resolve } from 'node:path';
import { build } from 'esbuild';

/** 最终安装包的正式browser导出及完整依赖闭包；不使用源码alias或工作区模块。 */
export async function buildConsumerBrowser(directory) {
  const coreRoot = await realpath(join(directory, 'node_modules/@hanger-source/dsh-reactive'));
  const clientRoot = await realpath(join(directory, 'node_modules/@hanger-source/dsh-reactive-client'));
  const client = JSON.parse(await readFile(join(clientRoot, 'package.json'), 'utf8'));
  const browserEntry = client.exports['.'].browser;
  if (typeof browserEntry !== 'string') throw new Error('RELEASE_BROWSER_EXPORT_REQUIRED');
  const output = 'browser/consumer.js';
  const result = await build({ absWorkingDir: directory, entryPoints: ['compiled/browser.js'], outfile: output,
    platform: 'browser', format: 'esm', target: 'es2024', conditions: ['browser', 'import'], bundle: true,
    metafile: true, logLevel: 'silent', logOverride: { 'import-is-undefined': 'error' } });
  const inputs = Object.keys(result.metafile.inputs).map(input => resolve(directory, input));
  if (inputs.some(input => input.replaceAll('\\', '/').includes('/node_modules/react-reconciler/')))
    throw new Error('RELEASE_BROWSER_BACKEND_RENDERER');
  if (!inputs.includes(resolve(clientRoot, browserEntry))) throw new Error('RELEASE_BROWSER_EXPORT_NOT_CONSUMED');
  const coreFiles = inputs.filter(input => input.replaceAll('\\', '/').includes('/node_modules/@hanger-source/dsh-reactive/'));
  if (!coreFiles.length) throw new Error('RELEASE_BROWSER_CORE_NOT_CONSUMED');
  for (const input of coreFiles) {
    const child = relative(coreRoot, await realpath(input));
    if (child === '..' || child.startsWith('../') || child.startsWith('..\\') || isAbsolute(child))
      throw new Error('RELEASE_BROWSER_CORE_IDENTITY_MISMATCH');
  }
  const emitted = Object.values(result.metafile.outputs);
  if (emitted.some(file => file.imports.some(imported => imported.external))) throw new Error('RELEASE_BROWSER_EXTERNAL_IMPORT');
  return { entry: relative(directory, resolve(clientRoot, browserEntry)), output: join(directory, output),
    bytes: emitted.reduce((sum, file) => sum + file.bytes, 0), modules: inputs.length, coreModules: coreFiles.length };
}
