import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';
import { build } from 'esbuild';

const directory = fileURLToPath(new URL('../', import.meta.url));
// dsh /client 的 browser 条件是自动安装插件的无导出产物；组合式 SDK
// 消费其正式 ESM 导出条件，浏览器平台仍负责拒绝 Node 内置依赖。
const result = await build({ absWorkingDir: directory, entryPoints: {
  index: 'lib/index.js', types: 'lib/contracts/types.js', errors: 'lib/contracts/errors.js',
  inbox: 'lib/contracts/inbox.js', transport: 'lib/transport.js', remote: 'lib/typert.remote-client.js',
}, outdir: 'lib/browser', entryNames: '[name]', chunkNames: 'chunks/[name]-[hash]', splitting: true,
  bundle: true, format: 'esm', platform: 'browser', target: 'es2024', conditions: ['node', 'import'],
  external: ['@hanger-source/dsh-reactive'], define: { 'process.env.NODE_ENV': '"production"' }, metafile: true, sourcemap: true,
  logOverride: { 'import-is-undefined': 'error' } });
const outputs = new Set(Object.keys(result.metafile.outputs).map(file => resolve(directory, file)));
for (const output of Object.values(result.metafile.outputs)) for (const imported of output.imports) {
  // 只有同一次构建签发的 chunk 和唯一 core 依赖可以留在输出关系中。
  if (imported.external ? imported.path !== '@hanger-source/dsh-reactive' : !outputs.has(resolve(directory, imported.path))) {
    throw new Error(`REACTIVE_CLIENT_BROWSER_EXTERNAL_INVALID:${imported.path}`);
  }
}
