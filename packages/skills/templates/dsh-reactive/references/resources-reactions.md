# 资源与持续反应

适用版本：`{{PACKAGE_VERSION}}`。纯入口为 `@hanger-source/dsh-reactive`，作用域也可从 `@hanger-source/dsh-reactive/reactivity` 导入；这里不需要 React 或 dsh 宿主。

## 选择对象与共享范围

- 可复用合同：`defineResource({ name, params, value })` → `implementResource(definition, implementation)` → `bindResource(implementation, { scope }).instance(params)`。
- 局部事实：`createResource(scope, { value, read, invalidations? })`，或互斥的 `{ observe }` 分支。它返回相同 `ResourceInstance`，不是另一套状态。可选 name 是诊断标签，不决定共享身份，也不需要注册名称。
- 已有事实读侧：`new ResourceReader({ observations$, peek })` 沿用来源寿命，不另建 Scope、缓存或查询；提供第二参数 `{ signal }` 时限定本次借用寿命。调用活动等内存事实可以直接供 read、Observable 和 hooks 共用，执行取消信号不应被误用为已完成事实的读取寿命。问答、请求和 SDK 等有独立 owner 的工厂仍要求明确的所属作用域。
- `params` 的解析输出必须是 JSON；解析后的参数内容与**实现对象、Scope**共同决定实例身份。值可以是 Date 等领域数据，不要为共享把服务对象塞进 params。
- 将实现放在依赖装配处，复用它；在每次 hook 调用里重新 implement 会改变身份。业务服务依赖改变时创建新实现；业务目标改变时请求对应参数实例。

```ts
import { Scope, defineResource, implementResource, bindResource, z,
  type Observation } from '@hanger-source/dsh-reactive';
import type { Observable } from 'rxjs';

export const Stock = defineResource({ name: 'stock',
  params: z.object({ sku: z.string() }),
  value: z.object({ revision: z.number().int(), available: z.number().int() }) });

export function inventory(scope: Scope, service: {
  observe(sku: string, signal: AbortSignal): Observable<Observation<z.input<typeof Stock.value>>>;
}) {
  const implementation = implementResource(Stock, {
    observe: ({ sku }, { signal }) => service.observe(sku, signal),
  });
  return bindResource(implementation, { scope });
}
```

`instance.observations$`、`peek()`、`read(signal?)`、`subscribe(callback)`消费同一实例。`read` 暂时持有观察，signal 取消这次等待。构造不读；多个观察者共享一次连接；最后一个退出撤掉旧 ready，实例身份保留，重新观察重新读取。Scope 退出释放来源，不能继续读已失效快照。

一次读取的 owner 使用 `read(params, { signal })` 加 `invalidations(params, context)`；推送 owner 使用 `observe` 发出完整 Observation。失效通知只表示需重新读取，不携带替代事实。写入成功后可显式 `instance.invalidate()`；这不会重发写入或唤醒模型。

## 派生仍使用标准 Observable

`combineResources({ stock: stock.observations$, quote: quote.observations$ })` 只有所有依赖 ready 才产生派生 ready。`mapResource(project)` 保留 pending/error，并将投影异常表示为 error。

需要把完整派生关系作为对象传给普通函数、Promise、其他 hooks 或公开 View 时，用 `shareResource(combineResources(...), { signal: scope.signal })`。read、peek、subscribe、values$、observations$ 共用 RxJS 的唯一当前快照；已经解析的领域值不再重复 schema parse，不需要另建 Scope、合同或绑定表。构造不连接，有限来源的最终观察保留至最后消费者退出，之后显式读取再连接；signal 固定读侧寿命，取消不重新执行任何命令。

切换查询使用 `source$.pipe(switchResource(project, { dependencies: value => [value.sku, value.revision] }))`。只选真实影响读取的字段，元素按 Object.is 比较；框架捕获依赖 tuple，并保留连续相同有效依赖的连接。pending/error 会撤旧读取，同参数恢复 ready 也重新读取。不要手写 JSON.stringify、Observation 比较器或额外 query cache。

后台组件从 `@hanger-source/dsh-reactive/program` 调用 `useResource(instance)`，或 `useResource(implementation, params)`。后者按当前程序 Scope 共享同一实现与参数；跨会话共享时显式把绑定放到适当共享 Scope，再把实例传入。`useResource(factory, deps)` 用于已有 Observation 流，不让 hooks 成为唯一 owner。

需要普通函数读取、写后失效或将对象交给其他业务 hook 时，用 `useResourceInstance(implementation, params)` 取得相同实例，再按需 `useResource(instance)`。取得实例不启动读取；不要为 read、Context 和公开 View 各建一个资源。

局部 hook 可以直接 `useResourceInstance(() => ({ value: z.string(), read: () => target }), [target])`；observe/invalidations 与 createResource 使用同一选项和语义。局部反应直接 `useReaction(() => ({ event, result, observe, operator, execute }), dependencies)`，继续显式 start。factory 纯构造选项，dependencies 与 React useMemo 相同，必须列出捕获目标、数据源或业务函数的身份依赖；不要在 factory 中启动查询或动作。相同组件和依赖保留实例，其他组件不凭相同名称共享局部对象；共享合同仍用 implementation/params 入口。

局部对象由当前 Program 的响应式作用域持有，可把整个资源／反应组合从业务 hook 返回给其他模块、普通函数或公开 View。依赖改变后取得新对象，之前捕获的资源与显式反应运行保留原目标，不自动重定向或停止。组件退出只释放组件查询，Program 失败或退出才撤销这些对象的查询和感知；已受理业务保持原 owner。

## 反应是显式运行

可复用反应采用 `defineReaction({ name, params, event, result })`、`implementReaction`、`bindReaction`；局部采用 `createReaction(scope, options)`，name 可省略，多个局部对象仍各有自己的运行身份。两者都是 `instance.start()` 才执行。

程序内用 `useReaction(implementation, params)` 取得程序拥有的相同参数实例，不手工创建 Scope 或绑定表。hook 不自动 start；同实例的重复并行 start 会失败。组件卸载不停止程序拥有的运行，显式 `run.stop()` 才结束关系；Program 失败或退出会释放查询、感知和排队工作，已受理业务仍归原 owner。Reaction 执行失败由 run 结算，只有业务明确传播时才进入 Program 失败。

```ts
import { createReaction, z, type Scope } from '@hanger-source/dsh-reactive';
import { concatMap, filter, type Observable } from 'rxjs';

export function restock(scope: Scope, events: Observable<{ sku: string; revision: number }>,
  request: (event: { sku: string; revision: number }) => Promise<{ operationId: string }>) {
  return createReaction(scope, {
    event: z.object({ sku: z.string(), revision: z.number() }),
    result: z.object({ operationId: z.string() }),
    observe: () => events.pipe(filter(event => event.revision > 0)),
    operator: concatMap,
    execute: request,
  });
}
```

`operator` 直接接 `concatMap`、`mergeMap`、`switchMap`、`exhaustMap`。条件、去重和并发用 RxJS；不要另创策略字符串。execute 可返回普通值、PromiseLike 或 Observable，也可直接返回真实 Action 的 `invocation.result`。

`run.state$`、`run.events$`、`run.settled` 观察同一次运行。`events$` 只交付热的成功结果，不回放执行；运行终止时正常 complete。来源、执行或释放失败保存在 `state$` 的 failed 状态和 `settled` 的 failed 结算中，不进入 `events$` 的 error 回调，也不会使 `settled` reject。`state$` 回放当前状态，先 start 后订阅也能观察同步发生的失败。

来源或执行结果同步完成时，订阅返回并接收清理函数后才交付终止；这次连接传播出的清理失败仍进入 failed，不回滚已经发生的业务。来源内部自行吞掉的错误不由框架恢复，不要通过修改 RxJS 全局错误配置改变其他模块的语义。

```ts
const run = reaction.start();
run.state$.subscribe(state => {
  if (state.status === 'failed') console.error('restock.reaction.failed', state.error);
});
```

取消监控订阅不停止运行。未处理错误终止这次运行，重新 start 是显式决定。stop 或 Scope 退出停止感知及尚未开始的 RxJS 工作，不取消已受理 Action/Job。跨重启执行、事务幂等和定时调度归原业务或原生 owner。

持续事件需要提交会话时，先用宿主 `host.service(sessionId, Program)` 得到固定服务身份端口；调用其 `resolveAction` 或 `submit`，不能借人工句柄自报 actor 或继承人的 Goal。
