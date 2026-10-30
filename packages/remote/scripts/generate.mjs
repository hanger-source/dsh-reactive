import { fileURLToPath } from 'node:url';
import { existsSync, mkdirSync, readFileSync, realpathSync, renameSync, writeFileSync } from 'node:fs';
import { randomUUID } from 'node:crypto';
import { dirname, relative, resolve } from 'node:path';
import ts from 'typescript';
import { WorkspaceAnalyzer, WorkspaceCaches, FaceModelEmitter, writeWorkspaceArtifacts } from '@deepseek-ai/dsh-typert-generator';

const root = fileURLToPath(new URL('../../../', import.meta.url));
const name = '@hanger-source/dsh-reactive-remote';
const caches = new WorkspaceCaches();
const analyzer = new WorkspaceAnalyzer({ root, packages: [name], faces: ['host'], caches });
if (!analyzer.discoverPackages().some(entry => entry.package === name)) {
  throw new Error('REACTIVE_REMOTE_TYPERT_CONTRIBUTION_MISSING');
}

// 依赖贡献使用已经构建的公开声明；绝对 exports 保留 DTS 的真实模块身份，
// 避免工作区源码推导把 lib/contracts/types.d.ts 改成未加载的 src/contracts/types.ts。
const publishedTargets = (packageRoot, value) => typeof value === 'string' ? resolve(packageRoot, value)
  : Object.fromEntries(Object.entries(value).map(([key, target]) => [key, publishedTargets(packageRoot, target)]));

// 官方分析器用注册目录确认 Remote/JsonValue/文件合同的声明归属。公开 inventory
// 补入实际 npm 包；源码、符号、exports 和所有 endpoint metadata 仍由官方分析器读取。
const dependencies = [
  { name: '@deepseek-ai/dsh-typert-protocol' },
  { name: '@deepseek-ai/dsh-util-values' },
  { name: '@deepseek-ai/dsh-api-workspace-files', exportSubpaths: ['./types'] },
].map(({ name: dependency, exportSubpaths }) => {
  const manifestPath = fileURLToPath(import.meta.resolve(`${dependency}/package.json`));
  const packageRoot = realpathSync(dirname(manifestPath));
  const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
  const entry = fileURLToPath(import.meta.resolve(dependency));
  const parsed = ts.parseJsonConfigFileContent({ files: [manifest.types], compilerOptions: {} }, ts.sys, packageRoot);
  if (manifest.name !== dependency || parsed.errors.length) throw new Error(`REACTIVE_REMOTE_TYPERT_DEPENDENCY_INVALID:${dependency}`);
  // npm 的 public types 是实际分析输入，不把 lib/*.d.ts 当作尚未构建的 src/*.ts。
  return { face: 'host', name: dependency, root: packageRoot,
    ...(exportSubpaths ? { exportSubpaths } : {}),
    manifest: { ...manifest, exports: publishedTargets(packageRoot, manifest.exports) },
    config: { path: entry, parsed } };
});
for (const inventory of caches.registrations.values()) {
  const index = inventory.findIndex(registration => registration.name === name && registration.face === 'host');
  if (index < 0) throw new Error('REACTIVE_REMOTE_TYPERT_HOST_FACE_MISSING');
  // Host 实现与纯 wire 合同属于不同包，不能从 SDK 根或生成贡献反向扫描。
  inventory[index] = { ...inventory[index], exportSubpaths: ['.'] };
  const clientIndex = inventory.findIndex(registration => registration.name === '@hanger-source/dsh-reactive-client' && registration.face === 'host');
  if (clientIndex < 0) throw new Error('REACTIVE_CLIENT_TYPERT_CONTRACT_FACE_MISSING');
  const client = inventory[clientIndex];
  inventory[clientIndex] = { ...client, exportSubpaths: ['./types'],
    manifest: { ...client.manifest, exports: publishedTargets(client.root, client.manifest.exports) } };
  inventory.push(...dependencies);
}
const model = analyzer.analyze();
const artifacts = model.faces.flatMap(face => face.packages.filter(entry => entry.name === name)
  .map(entry => ({ ...new FaceModelEmitter(face).emit(name), packageRoot: entry.root })));
if (artifacts.length !== 1 || !artifacts[0].remote) throw new Error('REACTIVE_REMOTE_TYPERT_REMOTE_MISSING');
writeWorkspaceArtifacts(root, artifacts);

// schema、namespace 和 endpoint metadata 原样来自官方 Host FaceModel。
// Remote 贡献正式归客户端包；Host 不发布第二份客户端实现或贡献入口。
const hostOutput = resolve(root, 'packages/remote/lib');
const clientOutput = resolve(root, 'packages/client/lib');
mkdirSync(clientOutput, { recursive: true });
const declarationFile = resolve(hostOutput, 'typert.remote-client.d.ts');
const declaration = readFileSync(declarationFile, 'utf8');
const mapDirective = '//# sourceMappingURL=typert.remote-client.d.ts.map\n';
if (!declaration.endsWith('\n' + mapDirective)) {
  throw new Error('REACTIVE_CLIENT_TYPERT_DECLARATION_MAP_DIRECTIVE_INVALID');
}
// 客户端发行包不含 Host 源码；只移除官方末尾 map 指令，声明正文保持原样。
writeFileSync(declarationFile, declaration.slice(0, -mapDirective.length));
const archive = resolve(root, '.trash', 'build-output-' + randomUUID());
const archiveMap = source => {
  const saved = resolve(archive, relative(root, source));
  mkdirSync(dirname(saved), { recursive: true });
  renameSync(source, saved);
};
// 保留生成位置的目录镜像；增量构建遗留在客户端的 map 也必须退出发行目录。
archiveMap(resolve(hostOutput, 'typert.remote-client.d.ts.map'));
const previousMap = resolve(clientOutput, 'typert.remote-client.d.ts.map');
if (existsSync(previousMap)) archiveMap(previousMap);
for (const file of ['typert.remote-client.js', 'typert.remote-client.d.ts']) {
  renameSync(resolve(hostOutput, file), resolve(clientOutput, file));
}
