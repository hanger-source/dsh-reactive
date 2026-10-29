import { mkdir, readFile, realpath, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { dirname, isAbsolute, join, relative, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { build } from 'esbuild';
import { capture, root, run } from './common.mjs';

const nativeModules = ['@deepseek-ai/dsh-client-modules', '@deepseek-ai/dsh-client-connection',
  '@deepseek-ai/dsh-typert-registry', '@deepseek-ai/dsh-api-gateway', '@deepseek-ai/dsh-client-file-upload',
  '@deepseek-ai/dsh-api-session-controller', '@deepseek-ai/dsh-client-ui-renderer',
  '@deepseek-ai/dsh-api-remote-access', '@deepseek-ai/dsh-client-ui-session'];

function contains(directory, file) {
  const child = relative(directory, file);
  return child !== '..' && !child.startsWith('../') && !child.startsWith('..\\') && !isAbsolute(child);
}

/** 以最近的 node_modules 成员识别实际 owner，嵌套安装也不能形成第二份 React/core。 */
function packageOwner(file) {
  const normalized = file.replaceAll('\\', '/');
  const start = normalized.lastIndexOf('/node_modules/');
  if (start === -1) return undefined;
  const segments = normalized.slice(start + '/node_modules/'.length).split('/');
  const name = segments[0].startsWith('@') ? segments.slice(0, 2).join('/') : segments[0];
  return { name, directory: normalized.slice(0, start + '/node_modules/'.length) + name };
}

async function checkBrowserBundle(result, directory, expectedRoots) {
  const inputs = Object.keys(result.metafile.inputs).map(input => resolve(directory, input));
  const modules = new Map(Object.keys(expectedRoots).map(name => [name, 0]));
  for (const input of inputs) {
    const owner = packageOwner(await realpath(input));
    if (owner?.name === 'react-reconciler') throw new Error('RELEASE_BROWSER_BACKEND_RENDERER');
    if (!owner || !Object.hasOwn(expectedRoots, owner.name)) continue;
    if (owner.directory !== expectedRoots[owner.name].replaceAll('\\', '/')) {
      throw new Error('RELEASE_BROWSER_PACKAGE_IDENTITY_MISMATCH: ' + owner.name);
    }
    modules.set(owner.name, modules.get(owner.name) + 1);
  }
  for (const [name, count] of modules) {
    if (!count) throw new Error('RELEASE_BROWSER_PACKAGE_NOT_CONSUMED: ' + name);
  }
  const emitted = Object.values(result.metafile.outputs);
  if (emitted.some(file => file.imports.some(imported => imported.external))) throw new Error('RELEASE_BROWSER_EXTERNAL_IMPORT');
  return { inputs, emitted, coreModules: modules.get('@hanger-source/dsh-reactive'), modules };
}

/** 原生 UI 的正式 client bundles 与试验入口共享同一个仓外安装；不借后台 workspace 的 React。 */
async function buildNativeBrowser(directory, coreRoot) {
  const require = createRequire(join(directory, 'package.json'));
  const installed = async name => {
    const manifest = await realpath(require.resolve(name + '/package.json'));
    if (!contains(join(directory, 'node_modules'), manifest)) throw new Error('RELEASE_BROWSER_PACKAGE_OUTSIDE_CONSUMER: ' + name);
    return { directory: dirname(manifest), manifest: JSON.parse(await readFile(manifest, 'utf8')) };
  };
  const [react, reactDom] = await Promise.all([installed('react'), installed('react-dom')]);
  const publicEntry = (owner, key, condition) => {
    const face = owner.manifest.exports?.[key];
    const target = typeof face === 'string' ? face : face?.[condition];
    if (typeof target !== 'string' || !contains(owner.directory, resolve(owner.directory, target))) {
      throw new Error('RELEASE_BROWSER_PUBLIC_EXPORT_REQUIRED: ' + owner.manifest.name + key + ':' + condition);
    }
    return resolve(owner.directory, target);
  };
  const output = join(directory, 'browser/native');
  await mkdir(output, { recursive: true });
  const result = await build({ absWorkingDir: directory, entryPoints: { consumer: 'native/browser-native-consumer.mjs' },
  outdir: output, entryNames: '[name]', chunkNames: 'chunks/[name]-[hash]',
  platform: 'browser', format: 'esm', target: 'es2024', conditions: ['browser', 'import'], bundle: true,
  splitting: true, metafile: true, logLevel: 'silent', define: { 'process.env.NODE_ENV': '"production"' },
  logOverride: { 'import-is-undefined': 'error' } });
  const checked = await checkBrowserBundle(result, directory, {
    '@hanger-source/dsh-reactive': coreRoot, react: react.directory, 'react-dom': reactDom.directory,
  });
  const bootstrap = (await Promise.all(nativeModules.map(async name => {
    const owner = await installed(name);
    const condition = ['@deepseek-ai/dsh-client-connection', '@deepseek-ai/dsh-typert-registry', '@deepseek-ai/dsh-api-gateway']
      .includes(name) ? 'browser' : 'default';
    return readFile(publicEntry(owner, './client', condition), 'utf8');
  }))).join('\n');
  const bootstrapFile = 'native-bootstrap.js';
  await writeFile(join(output, bootstrapFile), bootstrap);
  const files = [...Object.keys(result.metafile.outputs).map(file => relative(output, resolve(directory, file)).replaceAll('\\', '/')),
    bootstrapFile].sort();
  const proof = { directory: output, entry: 'consumer.js', bootstrap: bootstrapFile, modules: nativeModules, files,
    bytes: checked.emitted.reduce((sum, file) => sum + file.bytes, Buffer.byteLength(bootstrap)),
    inputModules: checked.inputs.length, coreModules: checked.coreModules,
    reactModules: checked.modules.get('react'), reactDomModules: checked.modules.get('react-dom') };
  await writeFile(join(output, 'manifest.json'), JSON.stringify(proof, null, 2) + '\n');
  return proof;
}

/** 同一安装中的浏览器声明独立严格编译；拒绝显式或间接引入 Node ambient。 */
async function checkBrowserTypes(directory) {
  const output = await capture(process.execPath, [join(directory, 'node_modules/typescript/bin/tsc'),
    '-p', 'tsconfig.browser.json', '--listFiles'], directory);
  const files = output.split(/\r?\n/u).filter(Boolean);
  if (!files.length) throw new Error('RELEASE_BROWSER_TYPES_FILES_REQUIRED');
  const node = files.find(file => /\/node_modules\/@types\/node\//u.test(file.replaceAll('\\', '/')));
  if (node) throw new Error('RELEASE_BROWSER_NODE_AMBIENT: ' + node);
  return { strict: true, nodeAmbient: false, files: files.length };
}

/** 最终安装包的正式browser导出及完整依赖闭包；不使用源码alias或工作区模块。 */
export async function buildConsumerBrowser(directory) {
  const types = await checkBrowserTypes(directory);
  const coreRoot = await realpath(join(directory, 'node_modules/@hanger-source/dsh-reactive'));
  const clientRoot = await realpath(join(directory, 'node_modules/@hanger-source/dsh-reactive-client'));
  const client = JSON.parse(await readFile(join(clientRoot, 'package.json'), 'utf8'));
  const publicEntries = ['.', './errors', './types', './inbox', './transport', './remote'];
  const browserEntries = publicEntries.map(key => {
    const target = client.exports[key]?.browser;
    if (typeof target !== 'string') throw new Error('RELEASE_BROWSER_EXPORT_REQUIRED:' + key);
    return resolve(clientRoot, target);
  });
  const output = 'browser/consumer.js';
  const result = await build({ absWorkingDir: directory, entryPoints: ['compiled/browser.js'], outfile: output,
    platform: 'browser', format: 'esm', target: 'es2024', conditions: ['browser', 'import'], bundle: true,
    metafile: true, logLevel: 'silent', logOverride: { 'import-is-undefined': 'error' } });
  const { inputs, emitted, coreModules } = await checkBrowserBundle(result, directory, { '@hanger-source/dsh-reactive': coreRoot });
  for (const entry of browserEntries) if (!inputs.includes(entry)) throw new Error('RELEASE_BROWSER_EXPORT_NOT_CONSUMED:' + entry);
  const consumer = await import(pathToFileURL(join(directory, output)).href);
  if (!consumer.clientEntries) throw new Error('RELEASE_BROWSER_CLIENT_ENTRIES_REQUIRED');
  const { checkClientEntries } = await import(pathToFileURL(join(directory, 'runtime.mjs')).href);
  const clientEntries = await checkClientEntries(directory, consumer.clientEntries);
  return { entry: relative(directory, browserEntries[0]), entries: browserEntries.map(entry => relative(clientRoot, entry)),
    output: join(directory, output), clientEntries, types,
    bytes: emitted.reduce((sum, file) => sum + file.bytes, 0), modules: inputs.length, coreModules,
    native: await buildNativeBrowser(directory, coreRoot) };
}

/** 原生领域的纯值合同单独进浏览器；不借原生 UI 或已安装 Host 的运行闭包。 */
export async function buildNativeContractsBrowser(directory) {
  const coreRoot = await realpath(join(directory, 'node_modules/@hanger-source/dsh-reactive'));
  const hostRoot = await realpath(join(directory, 'node_modules/@hanger-source/dsh-reactive-dsh'));
  const output = 'browser/native-contracts.js';
  const result = await build({ absWorkingDir: directory, entryPoints: ['compiled/native-contracts-browser.js'], outfile: output,
    platform: 'browser', format: 'esm', target: 'es2024', conditions: ['browser', 'import'], bundle: true,
    metafile: true, logLevel: 'silent', logOverride: { 'import-is-undefined': 'error' } });
  const checked = await checkBrowserBundle(result, directory, {
    '@hanger-source/dsh-reactive': coreRoot, '@hanger-source/dsh-reactive-dsh': hostRoot,
  });
  const manifest = JSON.parse(await readFile(join(hostRoot, 'package.json'), 'utf8'));
  const hostEntries = new Set(Object.entries(manifest.exports).flatMap(([key, face]) => {
    if (key.endsWith('/client') || typeof face !== 'object') return [];
    const target = face.node ?? face.import ?? face.default;
    return typeof target === 'string' ? [resolve(hostRoot, target)] : [];
  }));
  if (checked.inputs.some(input => hostEntries.has(input))) throw new Error('RELEASE_NATIVE_CONTRACT_HOST_ENTRY');
  await import(pathToFileURL(join(directory, output)).href);
  return { output: join(directory, output), bytes: checked.emitted.reduce((sum, file) => sum + file.bytes, 0),
    modules: checked.inputs.length, coreModules: checked.coreModules };
}

/** Playwright 拥有浏览器和独立宿主进程；日常与正式发行执行同一份实包页面。 */
export async function executeConsumerBrowsers(consumers, output) {
  const backend = consumers.find(consumer => consumer.kind === 'backend');
  const ui = consumers.find(consumer => consumer.kind === 'ui');
  if (!backend || !ui?.browser?.native?.directory) throw new Error('RELEASE_BROWSER_CONSUMERS_REQUIRED');
  await mkdir(output, { recursive: true });
  const reportFile = join(output, 'report.json');
  await run(process.execPath, [join(root, 'node_modules/@playwright/test/cli.js'), 'test',
    '--config', join(root, 'test/browser/playwright.config.mjs')], root, { ...process.env,
    DSH_REACTIVE_BROWSER_BACKEND: backend.directory,
    DSH_REACTIVE_BROWSER_NATIVE_ASSETS: ui.browser.native.directory,
    DSH_REACTIVE_BROWSER_OUTPUT: output,
  });
  const report = JSON.parse(await readFile(reportFile, 'utf8'));
  const tests = suite => [...(suite.specs ?? []).flatMap(spec => spec.tests), ...(suite.suites ?? []).flatMap(tests)];
  const completed = report.suites.flatMap(tests);
  if (report.stats.expected !== 3 || report.stats.unexpected || report.stats.flaky || report.stats.skipped) {
    throw new Error('RELEASE_BROWSER_EXECUTION_INCOMPLETE');
  }
  const cases = [];
  for (const kind of ['sdk', 'native', 'requests']) {
    const selected = completed.filter(test => test.projectName === kind);
    const result = selected[0]?.results?.[0];
    const receipt = result?.attachments?.find(attachment => attachment.name === 'consumer-proof');
    if (selected.length !== 1 || selected[0].results.length !== 1 || result.status !== 'passed' || !receipt?.path) {
      throw new Error('RELEASE_BROWSER_EXECUTION_INCOMPLETE: ' + kind);
    }
    const proof = JSON.parse(await readFile(receipt.path, 'utf8'));
    if (proof?.passed !== true) throw new Error('RELEASE_BROWSER_EXECUTION_FAILED: ' + kind);
    cases.push({ kind, passed: true, proof });
  }
  return { runner: '@playwright/test', browser: 'chromium', report: reportFile, cases };
}
