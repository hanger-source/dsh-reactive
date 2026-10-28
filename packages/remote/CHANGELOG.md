# @hanger-source/dsh-reactive-remote

## 1.1.0

### Patch Changes

- Updated dependencies [66a1d07]
- Updated dependencies [7b3ef97]
- Updated dependencies [b4af2f5]
  - @hanger-source/dsh-reactive@1.1.0
  - @hanger-source/dsh-reactive-dsh@1.1.0
  - @hanger-source/dsh-reactive-client@1.1.0

## 1.0.0

### Patch Changes

- Updated dependencies [dd50bb1]
- Updated dependencies [bdd5279]
- Updated dependencies [60f19d6]
  - @hanger-source/dsh-reactive@1.0.0
  - @hanger-source/dsh-reactive-client@1.0.0
  - @hanger-source/dsh-reactive-dsh@1.0.0

## 0.20.2

### Patch Changes

- Updated dependencies [9aca405]
  - @hanger-source/dsh-reactive@0.20.2
  - @hanger-source/dsh-reactive-client@0.20.2
  - @hanger-source/dsh-reactive-dsh@0.20.2

## 0.20.1

### Patch Changes

- Updated dependencies [bdf0417]
  - @hanger-source/dsh-reactive-client@0.20.1
  - @hanger-source/dsh-reactive@0.20.1
  - @hanger-source/dsh-reactive-dsh@0.20.1

## 0.20.0

### Minor Changes

- fdab096: Action 以名称与正整数版本声明跨进程合同身份，新定义省略 version 时为 1。远程解析与业务受理前拒绝不匹配合同，原 Invocation 回执、原生展示与子程序委托恢复保留原版本，不再用当前同名合同解释历史结果。

  迁移时必须同步更新宿主与客户端。自建 wire 调用须提供 contract；原生 manifest 增加 actionVersion，reactiveAction metadata 格式升至 version 2 并增加 actionVersion，委托与子会话记录也要求 actionVersion。缺少这些字段的旧记录不能直接恢复；保留相应旧版本消费者，或由业务明确核验历史合同后进行数据迁移，框架不自动猜测版本。

### Patch Changes

- Updated dependencies [fdab096]
  - @hanger-source/dsh-reactive@0.20.0
  - @hanger-source/dsh-reactive-dsh@0.20.0
  - @hanger-source/dsh-reactive-client@0.20.0

## 0.19.2

### Patch Changes

- Updated dependencies [ebf227d]
  - @hanger-source/dsh-reactive-client@0.19.2
  - @hanger-source/dsh-reactive@0.19.2
  - @hanger-source/dsh-reactive-dsh@0.19.2

## 0.19.1

### Patch Changes

- Updated dependencies [84f447f]
  - @hanger-source/dsh-reactive@0.19.1
  - @hanger-source/dsh-reactive-client@0.19.1
  - @hanger-source/dsh-reactive-dsh@0.19.1

## 0.19.0

### Patch Changes

- Updated dependencies [f690a47]
  - @hanger-source/dsh-reactive@0.19.0
  - @hanger-source/dsh-reactive-dsh@0.19.0
  - @hanger-source/dsh-reactive-client@0.19.0

## 0.18.2

### Patch Changes

- Updated dependencies [65812d1]
  - @hanger-source/dsh-reactive-dsh@0.18.2
  - @hanger-source/dsh-reactive-client@0.18.2
  - @hanger-source/dsh-reactive@0.18.2

## 0.18.1

### Patch Changes

- Updated dependencies [48fbba0]
  - @hanger-source/dsh-reactive@0.18.1
  - @hanger-source/dsh-reactive-client@0.18.1
  - @hanger-source/dsh-reactive-dsh@0.18.1

## 0.18.0

### Patch Changes

- Updated dependencies [f13c00f]
  - @hanger-source/dsh-reactive-dsh@0.18.0
  - @hanger-source/dsh-reactive-client@0.18.0
  - @hanger-source/dsh-reactive@0.18.0

## 0.17.2

### Patch Changes

- d458636: requestStatus、pause、cancel 的远程回执等待随调用 Session 句柄或 SDK 退出及时结算，借用的公共 Context 和其他 SDK 继续可用。复用现有取消等待接点，并将所属信号传入正式 Host 合同；Host 异步取得真实句柄后、进入读取或原生控制前重新检查取消。已经进入原生控制的工作仍由 Host 结算，客户端退出不回滚控制或已受理业务。同版本开发指导说明这些等待和执行 owner 的边界。
- Updated dependencies [d458636]
  - @hanger-source/dsh-reactive-client@0.17.2
  - @hanger-source/dsh-reactive@0.17.2
  - @hanger-source/dsh-reactive-dsh@0.17.2

## 0.17.1

### Patch Changes

- Updated dependencies [d158683]
  - @hanger-source/dsh-reactive@0.17.1
  - @hanger-source/dsh-reactive-client@0.17.1
  - @hanger-source/dsh-reactive-dsh@0.17.1

## 0.17.0

### Patch Changes

- Updated dependencies [db574d1]
  - @hanger-source/dsh-reactive@0.17.0
  - @hanger-source/dsh-reactive-client@0.17.0
  - @hanger-source/dsh-reactive-dsh@0.17.0

## 0.16.0

### Patch Changes

- Updated dependencies [469c5c5]
  - @hanger-source/dsh-reactive-dsh@0.16.0
  - @hanger-source/dsh-reactive-client@0.16.0
  - @hanger-source/dsh-reactive@0.16.0

## 0.15.0

### Patch Changes

- Updated dependencies [cb08a3b]
  - @hanger-source/dsh-reactive-dsh@0.15.0
  - @hanger-source/dsh-reactive-client@0.15.0
  - @hanger-source/dsh-reactive@0.15.0

## 0.14.0

### Patch Changes

- Updated dependencies [8dc0fb9]
  - @hanger-source/dsh-reactive@0.14.0
  - @hanger-source/dsh-reactive-client@0.14.0
  - @hanger-source/dsh-reactive-dsh@0.14.0

## 0.13.0

### Patch Changes

- Updated dependencies [2a3e750]
  - @hanger-source/dsh-reactive-client@0.13.0
  - @hanger-source/dsh-reactive@0.13.0
  - @hanger-source/dsh-reactive-dsh@0.13.0

## 0.12.1

### Patch Changes

- 5270fb3: 客户端退出时结束工作区发现与会话创建、打开、恢复的在途回执等待，并向 transport 传递 SDK 取消信号。已经发出的创建保留原引用并报告 CreationUnknownError；释放借用 SDK 不关闭公共 Context，也不撤销 Host 已受理的工作。开发 reference 同步说明取消与创建恢复的归属。
- Updated dependencies [5270fb3]
  - @hanger-source/dsh-reactive-client@0.12.1
  - @hanger-source/dsh-reactive@0.12.1
  - @hanger-source/dsh-reactive-dsh@0.12.1

## 0.12.0

### Patch Changes

- Updated dependencies [5081230]
  - @hanger-source/dsh-reactive-client@0.12.0
  - @hanger-source/dsh-reactive@0.12.0
  - @hanger-source/dsh-reactive-dsh@0.12.0

## 0.11.1

### Patch Changes

- Updated dependencies [9fb4b1a]
- Updated dependencies [be26221]
  - @hanger-source/dsh-reactive@0.11.1
  - @hanger-source/dsh-reactive-client@0.11.1
  - @hanger-source/dsh-reactive-dsh@0.11.1

## 0.11.0

### Patch Changes

- Updated dependencies [19c9a36]
  - @hanger-source/dsh-reactive@0.11.0
  - @hanger-source/dsh-reactive-dsh@0.11.0
  - @hanger-source/dsh-reactive-client@0.11.0

## 0.10.0

### Patch Changes

- 8060a41: Program、本地和远程 SDK 提供同源 human 资源；question(callId) 返回固定原问题的可复用对象，将状态观察、回答和前台等待放在同一对象上。问题状态保留 active、foreground、settled 原事实，不建立新的阶段、计时器或批准权。可选原生 UI 完整保留 pending/error，问题控制沿原 UI generation 与 SDK 准入。服务端口的人工回答和 Goal 控制在运行时明确拒绝，绕过类型声明也不能借用人工身份。
- Updated dependencies [8060a41]
  - @hanger-source/dsh-reactive@0.10.0
  - @hanger-source/dsh-reactive-dsh@0.10.0
  - @hanger-source/dsh-reactive-client@0.10.0

## 0.9.1

### Patch Changes

- Updated dependencies [f6050e9]
  - @hanger-source/dsh-reactive-dsh@0.9.1
  - @hanger-source/dsh-reactive-client@0.9.1
  - @hanger-source/dsh-reactive@0.9.1

## 0.9.0

### Patch Changes

- Updated dependencies [7a31aa8]
  - @hanger-source/dsh-reactive@0.9.0
  - @hanger-source/dsh-reactive-client@0.9.0
  - @hanger-source/dsh-reactive-dsh@0.9.0

## 0.8.0

### Patch Changes

- Updated dependencies [7451ffd]
  - @hanger-source/dsh-reactive-dsh@0.8.0
  - @hanger-source/dsh-reactive-client@0.8.0
  - @hanger-source/dsh-reactive@0.8.0

## 0.7.0

### Patch Changes

- Updated dependencies [424b75d]
  - @hanger-source/dsh-reactive@0.7.0
  - @hanger-source/dsh-reactive-dsh@0.7.0
  - @hanger-source/dsh-reactive-client@0.7.0

## 0.6.0

### Patch Changes

- Updated dependencies [4596edd]
  - @hanger-source/dsh-reactive-dsh@0.6.0
  - @hanger-source/dsh-reactive-client@0.6.0
  - @hanger-source/dsh-reactive@0.6.0

## 0.5.0

### Patch Changes

- Updated dependencies [339cacf]
  - @hanger-source/dsh-reactive-dsh@0.5.0
  - @hanger-source/dsh-reactive-client@0.5.0
  - @hanger-source/dsh-reactive@0.5.0

## 0.4.0

### Patch Changes

- Updated dependencies [923671d]
- Updated dependencies [baaa98b]
  - @hanger-source/dsh-reactive@0.4.0
  - @hanger-source/dsh-reactive-dsh@0.4.0
  - @hanger-source/dsh-reactive-client@0.4.0

## 0.3.0

### Patch Changes

- 1448bec: 原生 Jobs、Schedule、Team、Files、Approval 和 Clock 接受宿主核验的 Program conversation，普通函数与业务 hooks 共用会话能力实例。原生观察延迟到首次订阅，Program 失败或退出只释放其来源观察，不取消已受理工作。根会话在 Program 构造前持久完成工作区归属，创建 ACK 前不签发外部句柄；本地与远程 open/resume 保留同一创建引用，沿原入口完成登记。恢复继续核验成员撤销；开发 Skill 同步说明来源与生命周期。
- Updated dependencies [1448bec]
- Updated dependencies [f713b74]
- Updated dependencies [e4273f8]
  - @hanger-source/dsh-reactive-dsh@0.3.0
  - @hanger-source/dsh-reactive-client@0.3.0
  - @hanger-source/dsh-reactive@0.3.0

## 0.2.2

### Patch Changes

- Updated dependencies [5f53764]
  - @hanger-source/dsh-reactive@0.2.2
  - @hanger-source/dsh-reactive-client@0.2.2
  - @hanger-source/dsh-reactive-dsh@0.2.2

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
