# 原生会话能力扩展

业务事实、命令与长期操作使用 core 的 Resource、Action 和 Operation。需要接入 dsh 已安装的原生服务时，从 `@hanger-source/dsh-reactive-dsh/native` 导入 `createNativeSessionCapability`。工厂使用内置能力相同的安装、来源核验和调用机制，不需要修改 Host 或导入内部权限对象。

```ts
import { Observable } from 'rxjs';
import type { ToolRuntime } from '@deepseek-ai/dsh-tools';
import { createNativeSessionCapability } from '@hanger-source/dsh-reactive-dsh/native';

const capability = createNativeSessionCapability({
  name: 'project-tool-catalog',
  services: ['tools'],
  session(source) {
    const catalog = source.agentResource({
      changes: agent => new Observable<void>(subscriber =>
        agent.ctx.on('tools/change', () => subscriber.next())),
      read: agent => {
        const tools: ToolRuntime | undefined = agent.ctx.get('tools');
        if (!tools) throw new Error('PROJECT_TOOLS_REQUIRED');
        return tools.schemas(agent);
      },
    });
    return { catalog };
  },
});
```

将 capability 交给 `assembly.native(capability)`、`implementNativeModule(..., { native: [capability] })` 或已有的 `nativeCapabilities(...)` 安装。安装后用 `capability.forSession(session)` 或原 Program conversation 取得业务对象。模块不需要另登记工具名、来源身份或查询 owner。

## 安装与来源

`name` 固定本安装的诊断身份，`services` 明确列出全部必需的对象或函数服务，至少包含一项。配置用的 primitive 值不能作为服务身份。工厂捕获配置数组与函数；后续修改调用者的数组不会更改已经构造的依赖。

每次 `forSession(source)` 都先核验 Host 登记的真实对象、读取资格及全部服务的原始 provider。普通 `provide` 对象按自身比较，Cordis trace 代理按其原对象比较；同名服务不能跨隔离域互相借用。相同会话 ID 不会合并不同句柄，也不能用字段相同的普通对象冒充来源。相同 exact 来源共享同一个冻结业务面；独立句柄拥有各自的读取寿命。

内置能力和公共扩展使用同一服务组边界。Files 的文件服务与 filesystem、Skills 的 registry 与工作目录、启用工具的 Jobs 及 Approval 的工具服务，均须属于这次能力安装的完整依赖组；仅主服务相同不能借用其他依赖不同的安装。查询与实际调用继续复核同一组身份，命中已有业务面也不跳过来源核验。

需要安装自有原生 provider 时提供同步 generator `setup(context, home)`，逐项 yield 正式 Cordis Fiber 或清理函数。`home` 来自安装入口，不从进程 cwd 或全局变量猜测。依赖 provider 未活跃或安装失败时沿原生安装路径回滚；不要使用 async generator 丢失 Fiber 与 disposer 的身份。

每次开始后续安装或执行下一段 setup 前，框架重新核验实际 Cordis Fiber 及其祖先的运行资格。祖先已经退出时，即使当前安装仍在等待，也不会开始下一项能力、provider 或宿主初始化；已经开始的安装与清理仍等待真实结算。应用装配取得的全部能力按原退出合同释放，尚未开始安装的实例也必须支持 dispose。安装成功返回与已取得能力的 context、invoke 同样核验祖先，不能借当前子 Fiber 的暂存状态继续执行。

Goal round 使用同一实际 agents、goals、sessions 服务组下唯一有效的官方 Goal Round Driver。框架在准备与步骤进入边界核验其真实安装和消息关联，Driver 负责 reservation、claimed 状态及 round 准入。扩展 `agent/pre-step` 时，应调用 `next()` 并保留原生校验决定；模型权限依赖这条完整的链式准入。

`session(source)` 同步构造资源和方法闭包。它不读取来源、不订阅、不执行命令，也不返回 Promise。实际观察、`context()`、`liveAgent()` 和 `invoke()` 在构造成功前拒绝执行；构造失败释放本次派生关系，不缓存一个不完整对象。

## 资源与调用

| Context 成员 | 责任 |
| --- | --- |
| `sessionId`、`signal` | 真实来源身份与本读侧的读取寿命；ID 本身不授予权限 |
| `assertCurrent()`、`context()` | 核验安装、来源与全部必需服务后，取得扩展作者使用的原生 Context |
| `liveAgent()`、`liveAgent$` | 观察真实驻留 Agent 与代际；冷读不加载 Agent，流随本读侧退出 |
| `resource(observe)` | 冷构造同源 ResourceReader，查询、Promise、Observable 与 hooks 共用现有资源机制 |
| `agentResource({ changes, read })` | 跟随实际 Agent 代际；冷状态 pending，变化取消旧读取，采用结果前再核验来源与代际 |
| `invoke(operation, run, options?)` | 显式开始一次真实原生调用，框架固定来源并返回同一次 NativeInvocation |
| `own(cleanup)` | 将本扩展关系的实际清理交给派生 owner；可提前结束这一登记，异步释放仍由原 owner 等待 |

业务面只返回领域资源及明确方法。Context、Agent、Scope 和安装 owner 的销毁权不应成为借用对象的出口。

`agentResource` 读取可结构化复制的原生事实；框架取得自有快照后共享观察。原生服务、函数与执行句柄留在扩展内部。

`invoke` 复用现有的 `result`、`activity`、`activity$` 和 `wait`。传入的 options.signal 约束开始前检查与默认等待；它不会被框架隐式传给 run 或解释为回滚。原生服务支持执行前取消时，由扩展在 run 中显式传入该信号。已开始的函数沿原 owner 结算，能力卸载等待实际调用完成。

NativeInvocation 的 running 不证明事务已经受理，returned 不证明业务 Operation 或 Goal 已完成。原生权限、CAS 与领域完成条件继续由正式服务决定；该工厂不签发模型工具身份、原输入或 Goal 续接权。

## 释放与失败

消费者退出自己的订阅、读取等待或独立 Session 句柄。来源或 provider 退出撤销原读侧，旧业务面不换到新来源或新 provider。

`own` 登记的异步清理同时进入能力 owner 与真实 Host 的读取关闭链。即使来源 Session 已经从活跃图收集，Host 退出仍等待该清理。清理失败按原 Scope、Host 与能力退出合同报告；调用失败由自身 Invocation 观察，不伪装为清理失败。

工厂的 `dispose()` 由安装 owner 调用。读取连接、扩展关系、原生调用和业务 Operation 保持各自生命周期；该入口不建立另一个执行器、持久队列或恢复调用栈。
