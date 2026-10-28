import { copyFile, mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { capture, root, run } from './common.mjs';

export const consumerKinds = ['backend', 'native', 'ui'];

export function consumerPackages(config, kind) {
  return kind === 'ui'
    ? ['@hanger-source/dsh-reactive', '@hanger-source/dsh-reactive-client', '@hanger-source/dsh-reactive-native-ui', '@hanger-source/dsh-reactive-skills']
    : config.packages.filter(item => item.name !== '@hanger-source/dsh-reactive-native-ui').map(item => item.name);
}

/** 日常CI与正式发行消费同一组环境；可选原生依赖只取自实际宿主tgz的peer声明。 */
export async function consumerEnvironment(config, kind, tarballs) {
  const ui = kind === 'ui';
  const dependencies = { react: ui ? config.consumerTooling.uiReact : config.consumerTooling.backendReact };
  if (!ui) dependencies['react-reconciler'] = config.consumerTooling.backendReconciler;
  if (kind === 'backend') {
    const core = JSON.parse(await capture('tar', ['-xOf', tarballs.core, 'package/package.json']));
    const version = core.dependencies?.rxjs;
    if (typeof version !== 'string' || !version) throw new Error('RELEASE_CONSUMER_DEPENDENCY_REQUIRED: rxjs');
    dependencies.rxjs = version;
  }
  if (ui) {
    const core = JSON.parse(await capture('tar', ['-xOf', tarballs.core, 'package/package.json']));
    const nativeUi = JSON.parse(await capture('tar', ['-xOf', tarballs.nativeUi, 'package/package.json']));
    dependencies['react-dom'] = config.consumerTooling.uiReact;
    // rc.3 的 client-store 将这两个运行时列为开发依赖；由原生 UI 平台显式提供。
    dependencies.zustand = config.consumerTooling.uiZustand;
    dependencies.immer = config.consumerTooling.uiImmer;
    for (const name of ['rxjs', 'zod']) {
      const version = core.dependencies?.[name];
      if (typeof version !== 'string' || !version) throw new Error('RELEASE_CONSUMER_DEPENDENCY_REQUIRED: ' + name);
      dependencies[name] = version;
    }
    // 产品合同直接暴露的原生依赖必须由被消费的实际产物声明。
    for (const name of ['@deepseek-ai/cordis', '@deepseek-ai/dsh-client-store', '@deepseek-ai/dsh-client-ui-slots',
      '@deepseek-ai/dsh-api-session-controller', '@deepseek-ai/dsh-client-file-upload', '@deepseek-ai/dsh-session',
      '@deepseek-ai/dsh-api-remote-access', '@deepseek-ai/dsh-client-ui-session']) {
      const version = nativeUi.dependencies?.[name];
      if (typeof version !== 'string' || !version) throw new Error('RELEASE_NATIVE_UI_DEPENDENCY_REQUIRED: ' + name);
      dependencies[name] = version;
    }
    // 这些依赖属于试验壳：renderer、模块装载及本次会话交互，不加入 native-ui 产品依赖。
    for (const name of ['@deepseek-ai/dsh-commands', '@deepseek-ai/dsh-subagent', '@deepseek-ai/dsh-client-modules',
      '@deepseek-ai/dsh-client-connection', '@deepseek-ai/dsh-typert-registry', '@deepseek-ai/dsh-api-gateway',
      '@deepseek-ai/dsh-client-ui-renderer']) {
      dependencies[name] = config.consumerTooling.uiDsh;
    }
  }
  if (kind === 'native') {
    const host = JSON.parse(await capture('tar', ['-xOf', tarballs.host, 'package/package.json']));
    for (const [name, meta] of Object.entries(host.peerDependenciesMeta ?? {})) {
      if (meta.optional !== true) continue;
      const version = host.peerDependencies?.[name];
      if (typeof version !== 'string' || !version) throw new Error('RELEASE_NATIVE_PEER_VERSION_REQUIRED: ' + name);
      dependencies[name] = version;
    }
  }
  return { dependencies, devDependencies: { typescript: config.consumerTooling.typescript,
    ws: config.consumerTooling.ws,
    '@types/node': config.consumerTooling.nodeTypes,
    '@types/react': ui ? config.consumerTooling.uiReactTypes : config.consumerTooling.backendReactTypes } };
}

const consumerSources = kind => [{ source: `consumer-${kind}.ts`, target: 'consumer.ts' },
  { source: 'consumer-session.ts', target: 'consumer-session.ts' },
  ...(kind === 'native' ? [{ source: 'native-contracts-browser.ts', target: 'native-contracts-browser.ts' }] : []),
  ...(kind === 'ui' ? [{ source: 'consumer-browser.ts', target: 'browser.ts' }] : [])];
const nativeBrowserSources = ['browser-native-consumer.mjs', 'browser-contracts.mjs'];

/** 仓外消费者与编译选项只有此处装配，日常CI和发行验收使用相同输入。 */
export async function prepareConsumerSources(directory, kind) {
  const sources = consumerSources(kind);
  for (const file of sources) {
    await copyFile(join(root, 'scripts/release', file.source), join(directory, file.target));
  }
  await copyFile(join(root, 'scripts/release/consumer-skill.mjs'), join(directory, 'consumer-skill.mjs'));
  await copyFile(join(root, 'scripts/release/consumer-runtime.mjs'), join(directory, 'runtime.mjs'));
  if (kind === 'ui') {
    await mkdir(join(directory, 'native'));
    for (const file of nativeBrowserSources) {
      await copyFile(join(root, 'packages/remote/test', file), join(directory, 'native', file));
    }
  }
  await writeFile(join(directory, 'tsconfig.json'), JSON.stringify({ compilerOptions: {
    target: 'ES2024', module: 'NodeNext', moduleResolution: 'NodeNext', strict: true,
    skipLibCheck: false, outDir: 'compiled', types: ['node'], jsx: 'react-jsx',
  }, include: sources.map(file => file.target) }, null, 2));
}

/** 与工作区共用成熟依赖检查规则；cwd使解析与package.json都属于实际仓外安装。 */
export async function checkConsumerDependencies(directory, kind) {
  const sources = consumerSources(kind).map(file => file.target);
  await run(process.execPath, [join(root, 'node_modules/dependency-cruiser/bin/dependency-cruiser.mjs'),
    '--config', join(root, '.dependency-cruiser.cjs'), ...sources, 'consumer-skill.mjs', 'runtime.mjs',
    ...sources.map(file => join('compiled', file.replace(/\.ts$/u, '.js'))),
    ...(kind === 'ui' ? nativeBrowserSources.map(file => join('native', file)) : [])], directory);
}

/** 发行读取完整消费证据，不能只凭来源字段把缺项报告当作通过。 */
export function assertConsumerVerification(proof, config) {
  if (!Array.isArray(proof?.consumers) || proof.consumers.length !== consumerKinds.length) {
    throw new Error('RELEASE_CONSUMER_VERIFICATION_REQUIRED');
  }
  for (const kind of consumerKinds) {
    const consumer = proof.consumers.find(entry => entry?.kind === kind);
    const packages = consumerPackages(config, kind);
    if (!consumer || consumer.strict !== true || consumer.runtimeImport !== true || consumer.declaredDependencies !== true
      || !Array.isArray(consumer.packages) || consumer.packages.length !== packages.length
      || packages.some(name => !consumer.packages.includes(name))) throw new Error('RELEASE_CONSUMER_VERIFICATION_REQUIRED: ' + kind);
    if (kind === 'ui' && !['bytes', 'modules', 'coreModules'].every(key =>
      Number.isInteger(consumer.browser?.[key]) && consumer.browser[key] > 0)) {
      throw new Error('RELEASE_BROWSER_VERIFICATION_REQUIRED');
    }
    if (kind === 'native' && !['bytes', 'modules', 'coreModules'].every(key =>
      Number.isInteger(consumer.nativeContractsBrowser?.[key]) && consumer.nativeContractsBrowser[key] > 0)) {
      throw new Error('RELEASE_NATIVE_CONTRACTS_VERIFICATION_REQUIRED');
    }
    if (kind === 'ui') {
      const native = consumer.browser.native;
      if (!native || !['bytes', 'inputModules', 'coreModules', 'reactModules', 'reactDomModules'].every(key =>
        Number.isInteger(native[key]) && native[key] > 0)
        || !Array.isArray(native.modules) || !native.modules.includes('@deepseek-ai/dsh-client-ui-renderer')
        || !Array.isArray(native.files) || !native.files.includes(native.entry) || !native.files.includes(native.bootstrap)) {
        throw new Error('RELEASE_NATIVE_BROWSER_VERIFICATION_REQUIRED');
      }
    }
  }
}
