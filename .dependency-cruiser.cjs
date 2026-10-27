// 工作区依赖方向；发行 exports 和实际安装闭包另由 publint / registry 消费验证。
module.exports = {
  forbidden: [
    {
      name: 'declared-package-dependency', severity: 'error',
      from: {}, to: { dependencyTypes: ['npm-no-pkg', 'npm-unknown'] },
    },
    {
      name: 'published-runtime-dependency', severity: 'error',
      from: { path: '^packages/[^/]+/src/' },
      to: { dependencyTypes: ['npm-dev'], dependencyTypesNot: ['npm', 'npm-peer', 'npm-optional', 'type-only'] },
    },
    {
      name: 'package-relative-import-boundary', severity: 'error',
      from: { path: '^packages/([^/]+)/src/' },
      to: { dependencyTypes: ['local'], dependencyTypesNot: ['aliased'], pathNot: '^packages/$1/' },
    },
    {
      name: 'unresolved-import', severity: 'error',
      from: {}, to: { couldNotResolve: true },
    },
    {
      name: 'core-does-not-load-app-hosts', severity: 'error',
      from: { path: '^packages/core/src/' },
      to: { path: '^packages/(dsh|client|remote|native-ui|skills)/|node_modules/@hanger-source/dsh-reactive-(dsh|client|remote|native-ui|skills)(/|$)' },
    },
    {
      name: 'pure-core-does-not-load-renderer', severity: 'error',
      from: { path: '^packages/core/src/', pathNot: '^packages/core/src/(program/|kernel\\.ts$)' },
      to: { path: '^packages/core/src/program/(?!(definition|decode)\\.ts$)|node_modules/(react|react-reconciler)(/|$)',
        dependencyTypesNot: ['type-only'] },
    },
    {
      name: 'dsh-does-not-load-consumers', severity: 'error',
      from: { path: '^packages/dsh/src/' },
      to: { path: '^packages/(client|remote|native-ui|skills)/|node_modules/@hanger-source/dsh-reactive-(client|remote|native-ui|skills)(/|$)' },
    },
    {
      name: 'client-does-not-load-host-or-ui', severity: 'error',
      from: { path: '^packages/client/src/' },
      to: { path: '^packages/(dsh|remote|native-ui|skills)/|node_modules/@hanger-source/dsh-reactive-(dsh|remote|native-ui|skills)(/|$)|node_modules/(react|react-reconciler)(/|$)' },
    },
    {
      name: 'native-ui-does-not-load-host', severity: 'error',
      from: { path: '^packages/native-ui/src/' },
      to: { path: '^packages/(dsh|remote|skills)/|node_modules/@hanger-source/dsh-reactive-(dsh|remote|skills)(/|$)' },
    },
    {
      name: 'remote-does-not-load-ui', severity: 'error',
      from: { path: '^packages/remote/src/' },
      to: { path: '^packages/(native-ui|skills)/|node_modules/@hanger-source/dsh-reactive-(native-ui|skills)(/|$)' },
    },
  ],
  options: {
    doNotFollow: { path: '(^|/)node_modules/|^packages/[^/]+/lib/' },
    tsPreCompilationDeps: true,
    tsConfig: { fileName: 'tsconfig.json' },
    enhancedResolveOptions: { exportsFields: ['exports'], conditionNames: ['types', 'import', 'node', 'default'] },
  },
};
