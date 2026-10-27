# @hanger-source/dsh-reactive-remote

## 0.2.1

### Patch Changes

- ed8c8dc: 明确六包 TypeScript 构建依赖与客户端合同的唯一产物来源，严格编译并执行仓外消费者；以 GitHub OIDC 发布可追溯的同步版本及开发 Skill，并从 npm registry 安装验收。
- Updated dependencies [ed8c8dc]
- Updated dependencies [871403f]
  - @hanger-source/dsh-reactive@0.2.1
  - @hanger-source/dsh-reactive-dsh@0.2.1
  - @hanger-source/dsh-reactive-client@0.2.1

## 0.2.0

首次公开源码。

- 提供已认证 Gateway 宿主，将 Program、输入、Action、Operation、人工参与和进度映射到正式远程合同。
- 复用原生连接与上传服务，保留真实调用者、准入失败和受理未知事实。
