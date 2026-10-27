import { copyFile, writeFile } from 'node:fs/promises';
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
    '@types/node': config.consumerTooling.nodeTypes,
    '@types/react': ui ? config.consumerTooling.uiReactTypes : config.consumerTooling.backendReactTypes } };
}

const consumerSources = kind => [{ source: `consumer-${kind}.ts`, target: 'consumer.ts' },
  ...(kind === 'ui' ? [{ source: 'consumer-browser.ts', target: 'browser.ts' }] : [])];

/** 仓外消费者与编译选项只有此处装配，日常CI和发行验收使用相同输入。 */
export async function prepareConsumerSources(directory, kind) {
  const sources = consumerSources(kind);
  for (const file of sources) {
    await copyFile(join(root, 'scripts/release', file.source), join(directory, file.target));
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
    '--config', join(root, '.dependency-cruiser.cjs'), ...sources,
    ...sources.map(file => join('compiled', file.replace(/\.ts$/u, '.js')))], directory);
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
  }
}
