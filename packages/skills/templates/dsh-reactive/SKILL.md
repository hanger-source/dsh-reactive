---
name: dsh-reactive
description: 使用 dsh-reactive 构建响应式服务、Agent 程序和多会话应用；选择资源、反应、Action、Operation、模块、宿主与客户端的正式接点。适用于编写、组合或修改框架应用，不是运行时业务 Skill。
metadata:
  version: "{{PACKAGE_VERSION}}"
---

# dsh-reactive 应用开发

适用版本：`{{PACKAGE_VERSION}}`。六个 `@hanger-source/dsh-reactive*` 包固定同版；先核应用实际安装版本与公开声明，不能用另一版本的示例猜 API。此 Skill 随对应包构建，不代表已经全局注册。

公开源码与分域文档位于 [hanger-source/dsh-reactive](https://github.com/hanger-source/dsh-reactive)。从 npm 获取依赖时使用公共 registry 的实际可用版本；仓库中的 package version 不能证明已经发布，也不要求使用者配置私有 registry。

定位当前安装的 Skill：运行 `dsh-reactive-skill`，或从 `@hanger-source/dsh-reactive-skills` 调用 `getDeveloperSkill()`；两者仅返回版本和包内路径。

框架支持普通服务代码、Promise/回调、标准 Observable、后台业务 hooks 和可选原生 UI 共用编程对象。先确定事实、副作用和生命周期的 owner，再选入口；无需为了查询创建会话或渲染组件。

按当前任务读取对应参考，勿一次加载全部：

- 事实来源、共享参数实例、失效、派生和持续感知：[resources-reactions.md](references/resources-reactions.md)。
- 业务 Action、后台 Program、局部模块与动态集合：[program-action-module.md](references/program-action-module.md)。
- 长业务、可靠多阶段反馈、人工补充与 Goal 关联：[operation-human.md](references/operation-human.md)。
- 模型、Runtime、工作区、原生能力与子程序：[host-workspaces-native.md](references/host-workspaces-native.md)。
- 远程消费、输入与附件、绑定释放、可选原生 UI：[client-native-ui.md](references/client-native-ui.md)。

贯穿各入口的约束：

- 从纯核心导共享合同，从 `@hanger-source/dsh-reactive/program` 导后台 React 适配器；客户端不得导宿主、凭据或后台 renderer。原生宿主与 UI 分包。
- 远程 SDK 工厂与 retainNativeSession 使用 `defineProgram({ name, version, input, surface })` 签发的共享 Program，version 默认 1，与包版本分开。宿主核实际目标身份，客户端核取得回执；纯本地 Program 和模块 Surface 不强迫命名，通用会话消费者仍接受 ProgramContract。
- 构造定义、实现、实例、订阅和公开出口不等于业务命令。Action 调用与 Reaction `start()` 显式执行，渲染或读取不重放写入。
- 业务服务拥有权限、权威事实、正式完成和幂等；框架接走实例共享、声明与调用关联。dsh 拥有 inbox、工具、Goal 和子 Agent，不再创建第二套队列或持久调度器。
- 保留 `Observation` 的 pending/error 和真实输入来源。不把上次 ready、当前页面目标、工具字符串或模型文字冒充权威事实。
- 根据已安装 DTS 和应用调用验证新代码，再运行与变更相关的真实边界；编译、网络链、原生运行与外部业务验收分别报告。不存在的 owner 能力不能用假 ACK 补齐。
