import { fileURLToPath } from 'node:url';
import { build } from 'esbuild';

const directory = fileURLToPath(new URL('../', import.meta.url));
// dsh /client 的 browser 条件是自动安装插件的无导出产物；组合式 SDK
// 消费其正式 ESM 导出条件，浏览器平台仍负责拒绝 Node 内置依赖。
const result = await build({ absWorkingDir: directory, entryPoints: ['lib/index.js'], outfile: 'lib/client.browser.js',
  bundle: true, format: 'esm', platform: 'browser', target: 'es2024', conditions: ['node', 'import'],
  external: ['@hanger-source/dsh-reactive'], define: { 'process.env.NODE_ENV': '"production"' }, metafile: true, sourcemap: true,
  logOverride: { 'import-is-undefined': 'error' } });
for (const output of Object.values(result.metafile.outputs)) for (const imported of output.imports) {
  if (imported.path !== '@hanger-source/dsh-reactive') throw new Error(`REACTIVE_CLIENT_BROWSER_EXTERNAL_INVALID:${imported.path}`);
}
