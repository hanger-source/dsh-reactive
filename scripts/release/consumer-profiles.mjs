import { copyFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { capture, root } from './common.mjs';

export const consumerKinds = ['backend', 'native', 'ui'];

export function consumerPackages(config, kind) {
  return kind === 'ui'
    ? ['@hanger-source/dsh-reactive', '@hanger-source/dsh-reactive-client', '@hanger-source/dsh-reactive-native-ui', '@hanger-source/dsh-reactive-skills']
    : config.packages.filter(item => item.name !== '@hanger-source/dsh-reactive-native-ui').map(item => item.name);
}

/** 日常CI与正式发行消费同一组环境；可选原生依赖只取自实际宿主tgz的peer声明。 */
export async function consumerEnvironment(config, kind, hostTarball) {
  const ui = kind === 'ui';
  const dependencies = { react: ui ? config.consumerTooling.uiReact : config.consumerTooling.backendReact };
  if (!ui) dependencies['react-reconciler'] = config.consumerTooling.backendReconciler;
  if (kind === 'native') {
    const host = JSON.parse(await capture('tar', ['-xOf', hostTarball, 'package/package.json']));
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

/** 仓外消费者与编译选项只有此处装配，日常CI和发行验收使用相同输入。 */
export async function prepareConsumerSources(directory, kind) {
  await copyFile(join(root, `scripts/release/consumer-${kind}.ts`), join(directory, 'consumer.ts'));
  const include = ['consumer.ts'];
  if (kind === 'ui') {
    await copyFile(join(root, 'scripts/release/consumer-browser.ts'), join(directory, 'browser.ts'));
    include.push('browser.ts');
  }
  await writeFile(join(directory, 'tsconfig.json'), JSON.stringify({ compilerOptions: {
    target: 'ES2024', module: 'NodeNext', moduleResolution: 'NodeNext', strict: true,
    skipLibCheck: false, outDir: 'compiled', types: ['node'], jsx: 'react-jsx',
  }, include }, null, 2));
}
