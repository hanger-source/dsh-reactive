# @hanger-source/dsh-reactive-dsh

## 0.12.1

### Patch Changes

- @hanger-source/dsh-reactive@0.12.1

## 0.12.0

### Patch Changes

- @hanger-source/dsh-reactive@0.12.0

## 0.11.1

### Patch Changes

- Updated dependencies [9fb4b1a]
- Updated dependencies [be26221]
  - @hanger-source/dsh-reactive@0.11.1

## 0.11.0

### Minor Changes

- 19c9a36: Program、本地与远程 SDK 提供 progress 资源及 Request.state，统一当前读取、Observable 与 hooks 的消费，沿用原请求关联、原生活跃前缀和会话连接。原生 UI 按需借入相同读侧，绑定本身不额外启动进展连接；单次读取取消和 UI 释放不停止原生工作。createRequestHandle 的 progress 参数改为资源对象并显式接收读侧 owner；框架入口自动完成这些装配。

### Patch Changes

- Updated dependencies [19c9a36]
  - @hanger-source/dsh-reactive@0.11.0

## 0.10.0

### Minor Changes

- 8060a41: Program、本地和远程 SDK 提供同源 human 资源；question(callId) 返回固定原问题的可复用对象，将状态观察、回答和前台等待放在同一对象上。问题状态保留 active、foreground、settled 原事实，不建立新的阶段、计时器或批准权。可选原生 UI 完整保留 pending/error，问题控制沿原 UI generation 与 SDK 准入。服务端口的人工回答和 Goal 控制在运行时明确拒绝，绕过类型声明也不能借用人工身份。

### Patch Changes

- Updated dependencies [8060a41]
  - @hanger-source/dsh-reactive@0.10.0

## 0.9.1

### Patch Changes

- f6050e9: 为 dsh 直接使用的 LLM/附件合同和原生 UI 的公开 Session 类型补齐正式依赖，避免借用工作区或传递安装。依赖边界检查同时核验声明、运行时 devDependency 与跨包相对导入。日常 CI 和发行验收共用仓外编译配置，并从实际安装包按 browser 条件完整打包纯合同、SDK 与可选 UI，检查外部导入、后台 renderer 和 core 模块身份。
- @hanger-source/dsh-reactive@0.9.1

## 0.9.0

### Patch Changes

- Updated dependencies [7a31aa8]
  - @hanger-source/dsh-reactive@0.9.0

## 0.8.0

### Minor Changes

- 7451ffd: Jobs、Schedule、Team 的会话 SDK 和固定任务提供 state 资源，Job 的 output 提供非消费输出资源；函数、Promise、RxJS、hooks 和公开 View 共用正式资源机制。Schedule 与 Team 的原生变化先撤销旧 ready，异步读取失败作为可恢复的 error，后续通知继续读取。最后资源消费者退出取消读取且不停止已受理原生执行；各域的控制、单次权威查询和持久性责任保持独立。

### Patch Changes

- @hanger-source/dsh-reactive@0.8.0

## 0.7.0

### Minor Changes

- 424b75d: 本地、远程及动态集合提供稳定的 viewResource 读侧对象，普通函数、Promise、Observable、程序 hooks 与可选原生 UI 共用现有 View 状态和 SDK 读取寿命。ResourceReader 统一已有快照的读取、取消与终止结算，不建立新缓存或来源连接。自定义 SurfaceCollectionTransport 以 viewResource 返回已有读侧，取代分开的 observeView/readView 传输方法。

### Patch Changes

- Updated dependencies [424b75d]
  - @hanger-source/dsh-reactive@0.7.0

## 0.6.0

### Minor Changes

- 4596edd: Files 的文本页、元数据与目录提供同一正式 Resource 实例，支持普通 read、共享观察和程序 hooks。首次观察在原生 watch ready 后读取，同路径查询共享 watch，失效取消旧读，查询退出释放原生监听；目录变化不按目录版本过滤。文件范围直接读取已核验会话 header，不再为文件 I/O 读取或冲刷会话日志，关闭 Agent 不终止仍有效来源的文件观察。独立字节缓冲区与异步 unwatch 保留原边界，产物沿既有资源 Surface 对本地和远程公开，开发 Skill 与仓外类型消费者同步。

### Patch Changes

- @hanger-source/dsh-reactive@0.6.0

## 0.5.0

### Minor Changes

- 339cacf: Team 提供经原任务板核验的固定任务对象 task(id)，state$、read 和显式 update 共用原生投影与 CAS。冷 Lead 可读取、绑定，删除后原对象读侧返回 undefined，写入仍要求实际活跃成员，调用参数不能改指其他任务。Team 投影观察接入实际 Agent 关闭、恢复，取消在途旧切面，不复制任务状态、不创建成员或启动模型；开发 Skill 同步组合与生命周期语义。

  日常 CI 与发行增加可选原生域的仓外消费者，共用环境选择，从实际宿主 tgz 的 optional peer 声明安装所选依赖，严格编译并实际加载七个原生子入口；默认后台与原生 UI 的独立安装验证继续保留。

### Patch Changes

- @hanger-source/dsh-reactive@0.5.0

## 0.4.0

### Patch Changes

- baaa98b: 原生长期切面和日志观察跟随实际 Agent 的关闭与恢复，取消旧 owner 的在途读取并接续当前原生日志，避免迟到切面覆盖新现场或重复交付事件。原生挂载只保留一份可观察引用，输入、人工等待与 Operation 共用该事实；观察不自动恢复会话或启动模型，真正读取错误保持原语义。开发 Skill 同步说明观察寿命。
- Updated dependencies [923671d]
  - @hanger-source/dsh-reactive@0.4.0

## 0.3.0

### Minor Changes

- 1448bec: 原生 Jobs、Schedule、Team、Files、Approval 和 Clock 接受宿主核验的 Program conversation，普通函数与业务 hooks 共用会话能力实例。原生观察延迟到首次订阅，Program 失败或退出只释放其来源观察，不取消已受理工作。根会话在 Program 构造前持久完成工作区归属，创建 ACK 前不签发外部句柄；本地与远程 open/resume 保留同一创建引用，沿原入口完成登记。恢复继续核验成员撤销；开发 Skill 同步说明来源与生命周期。

### Patch Changes

- Updated dependencies [f713b74]
- Updated dependencies [e4273f8]
  - @hanger-source/dsh-reactive@0.3.0

## 0.2.2

### Patch Changes

- Updated dependencies [5f53764]
  - @hanger-source/dsh-reactive@0.2.2

## 0.2.1

### Patch Changes

- ed8c8dc: 明确六包 TypeScript 构建依赖与客户端合同的唯一产物来源，严格编译并执行仓外消费者；以 GitHub OIDC 发布可追溯的同步版本及开发 Skill，并从 npm registry 安装验收。
- Updated dependencies [ed8c8dc]
- Updated dependencies [871403f]
  - @hanger-source/dsh-reactive@0.2.1

## 0.2.0

首次公开源码。

- 提供原生 dsh 宿主、多工作区与会话、统一输入和文件上传、工具来源准入、人工问询及可靠 Operation 等待。
- 提供可选 Jobs、MCP、Approval、时间、现存 Schedule 管理、Team 任务板和工作区文件 SDK。
- Schedule 创建、Team spawn/send 与原生 Controller 输入前门仍受 dsh rc.3 公开接点限制。
