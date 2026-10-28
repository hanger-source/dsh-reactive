# SDK 作用域迁移

SDK 和 Session 使用独立的 dsh 原生 Fiber。取消使用权和等待实际清理分别表达，退出时由同一个 owner 汇总错误；资源、Operation、Invocation 和人工参与不再分别维护退出队列。

## 取得已安装的 SDK

`bindProgramClient(program, ctx)` 返回 `Promise<ProgramClient<P>>`。program 使用具名 defineProgram 签发的 SharedProgramContract，合同身份见[共享 Program 合同](migration-shared-program-contracts.md)。普通调用、原生扩展和自定义 SDK 工厂都要等待安装完成；失败通过 Promise 拒绝交付。

```ts
const client = await bindProgramClient(Program, ctx);
const session = await client.open(sessionId);
try {
  const facts = await session.readView('facts');
  consume(facts);
} finally {
  await session.release();
  await client.dispose();
}
```

`createProgramClient` 继续异步创建独立装配。`retainNativeSession` 已等待 SDK 作用域安装；应用仍等待该函数取得的原生引用，不再自行拼装新的 Connection。

## 通过会话取得 journal

`ProgramJournal` 由 `session.openEvents(options, request)` 创建，不直接调用构造函数。返回的句柄仍提供原生 journal 的打开、分页、重连与 dispose 接口；手动 dispose 也由原 Session 作用域持有，外层退出会加入已经开始的清理。

## 保留原 owner

Session release 立即撤销本句柄的查询和控制等待，并等待其原生资源静止；SDK dispose 等待所有 Session 及 SDK 持有的 Invocation 观察。它们不会取消已受理业务、删除 Operation 或重新执行 Action。

Action 受理 ACK 已确认时保留原 Invocation 身份，即使 SDK 在建立观察期间退出。此时 result 等待拒绝，仍可从句柄取得 id，再通过另一个有效 Session 的 `invocation(name, id)` 读取原结果。没有确认受理的调用继续使用明确的 admission-unknown 错误，不自动重发。

同一个公共 Context 中可以绑定多个 SDK，各有独立 Fiber。释放某个 SDK 不关闭公共 Context、Connection 或其他 SDK。core Scope 继续管理同步响应图，React 清理继续撤下自己的声明和订阅；网络异步释放由原生 Fiber 承担，不把它们塞进同步 effect。
