# @hanger-source/dsh-reactive-skills

## 0.2.1

### Patch Changes

- ed8c8dc: 明确六包 TypeScript 构建依赖与客户端合同的唯一产物来源，严格编译并执行仓外消费者；以 GitHub OIDC 发布可追溯的同步版本及开发 Skill，并从 npm registry 安装验收。
- 871403f: 明确 Reaction 失败通过 state$ 与 settled 观察，events$ 只交付成功结果；同步应用开发指导。
- Updated dependencies [ed8c8dc]
- Updated dependencies [871403f]
  - @hanger-source/dsh-reactive@0.2.1
  - @hanger-source/dsh-reactive-dsh@0.2.1
  - @hanger-source/dsh-reactive-client@0.2.1
  - @hanger-source/dsh-reactive-remote@0.2.1
  - @hanger-source/dsh-reactive-native-ui@0.2.1

## 0.2.0

首次公开源码。

- 提供与框架版本对应的开发者 Skill 及五份分域 references。
- 通过 getDeveloperSkill() 和 dsh-reactive-skill 定位安装内容，不复制到全局目录；五个框架包使用同版可选 peer。
