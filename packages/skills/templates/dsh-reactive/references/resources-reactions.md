# 资源与持续反应

适用版本：`{{PACKAGE_VERSION}}`。纯入口为 `@hanger-source/dsh-reactive`，作用域也可从 `@hanger-source/dsh-reactive/reactivity` 导入；这里不需要 React 或 dsh 宿主。

## 选择对象与共享范围

- 可复用合同：`defineResource({ name, params, value })` → `implementResource(definition, implementation)` → `bindResource(implementation, { scope }).instance(params)`。
- 局部事实：`createResource(scope, { name, value, read, invalidations? })`，或互斥的 `{ observe }` 分支。它返回相同 `ResourceInstance`，不是另一套状态。
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

切换查询使用 `source$.pipe(switchResource(project, { dependencies: value => [value.sku, value.revision] }))`。只选真实影响读取的字段，元素按 Object.is 比较；框架捕获依赖 tuple，并保留连续相同有效依赖的连接。pending/error 会撤旧读取，同参数恢复 ready 也重新读取。不要手写 JSON.stringify、Observation 比较器或额外 query cache。

后台组件从 `@hanger-source/dsh-reactive/program` 调用 `useResource(instance)`，或 `useResource(implementation, params)`。后者按当前程序 Scope 共享同一实现与参数；跨会话共享时显式把绑定放到适当共享 Scope，再把实例传入。`useResource(factory, deps)` 用于已有 Observation 流，不让 hooks 成为唯一 owner。

需要普通函数读取、写后失效或将对象交给其他业务 hook 时，用 `useResourceInstance(implementation, params)` 取得相同实例，再按需 `useResource(instance)`。取得实例不启动读取；不要为 read、Context 和公开 View 各建一个资源。

## 反应是显式运行

可复用反应采用 `defineReaction({ name, params, event, result })`、`implementReaction`、`bindReaction`；局部采用 `createReaction(scope, options)`。两者都是 `instance.start()` 才执行。

程序内用 `useReaction(implementation, params)` 取得程序拥有的相同参数实例，不手工创建 Scope 或绑定表。hook 不自动 start；同实例的重复并行 start 会失败。组件卸载不停止程序拥有的运行，显式 `run.stop()` 才结束关系；Program 失败或退出会释放查询、感知和排队工作，已受理业务仍归原 owner。Reaction 执行失败由 run 结算，只有业务明确传播时才进入 Program 失败。

```ts
import { createReaction, z, type Scope } from '@hanger-source/dsh-reactive';
import { concatMap, filter, type Observable } from 'rxjs';

export function restock(scope: Scope, events: Observable<{ sku: string; revision: number }>,
  request: (event: { sku: string; revision: number }) => Promise<{ operationId: string }>) {
  return createReaction(scope, {
    name: 'restock', event: z.object({ sku: z.string(), revision: z.number() }),
    result: z.object({ operationId: z.string() }),
    observe: () => events.pipe(filter(event => event.revision > 0)),
    operator: concatMap,
    execute: request,
  });
}
```

`operator` 直接接 `concatMap`、`mergeMap`、`switchMap`、`exhaustMap`。条件、去重和并发用 RxJS；不要另创策略字符串。execute 可返回普通值、PromiseLike 或 Observable，也可直接返回真实 Action 的 `invocation.result`。

`run.state$`、`run.events$`、`run.settled` 观察同一次运行。`events$` 只交付热的成功结果，不回放执行；运行终止时正常 complete。来源、执行或释放失败保存在 `state$` 的 failed 状态和 `settled` 的 failed 结算中，不进入 `events$` 的 error 回调，也不会使 `settled` reject。`state$` 回放当前状态，先 start 后订阅也能观察同步发生的失败。

```ts
const run = reaction.start();
run.state$.subscribe(state => {
  if (state.status === 'failed') console.error('restock.reaction.failed', state.error);
});
```

取消监控订阅不停止运行。未处理错误终止这次运行，重新 start 是显式决定。stop 或 Scope 退出停止感知及尚未开始的 RxJS 工作，不取消已受理 Action/Job。跨重启执行、事务幂等和定时调度归原业务或原生 owner。

持续事件需要提交会话时，先用宿主 `host.service(sessionId, Program)` 得到固定服务身份端口；调用其 `resolveAction` 或 `submit`，不能借人工句柄自报 actor 或继承人的 Goal。
