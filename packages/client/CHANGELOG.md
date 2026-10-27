# @hanger-source/dsh-reactive-client

## 0.2.1

### Patch Changes

- ed8c8dc: 明确六包 TypeScript 构建依赖与客户端合同的唯一产物来源，严格编译并执行仓外消费者；以 GitHub OIDC 发布可追溯的同步版本及开发 Skill，并从 npm registry 安装验收。
- Updated dependencies [ed8c8dc]
- Updated dependencies [871403f]
  - @hanger-source/dsh-reactive@0.2.1

## 0.2.0

首次公开源码。

- 提供独立类型化 Program 客户端、动态模块集合、按精确 scope 解析 Action、输入提交与编辑、进度和 Operation 读取。
- 取消选项保留请求、观察与业务执行各自的生命周期；客户端不加载宿主实现。
