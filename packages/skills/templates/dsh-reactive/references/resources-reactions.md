# 资源与持续反应

适用版本：`{{PACKAGE_VERSION}}`。纯入口为 `@hanger-source/dsh-reactive`，作用域也可从 `@hanger-source/dsh-reactive/reactivity` 导入；这里不需要 React 或 dsh 宿主。

## 选择对象与共享范围

显式 owner 的 ResourceReader 不以来源正常完成结束有效性观察。有限来源按时释放，既有观察继续接受 owner 撤销，避免外部 hook 或 Context 永久保留旧 ready；values$ 同样不承诺在业务返回时结束。空完成或 pending 后完成交付 EmptyError，不挂起读取。未指定 owner 的 ResourceReader 和普通 Observable 保留来源自己的有限语义；业务完成使用明确的 result/settled 或按业务状态筛选，不用流 EOF 猜测。

工作区共享使用已有 implementation：workspace.resource(implementation, params) 与 workspace.reaction(implementation, params) 直接返回正式 ResourceInstance、ReactionInstance。相同工作区、实现和已解析参数复用身份，跨 Environment 和 Session 共享，不同工作区隔离；构造不读，反应显式 start。组件从 props.workspace 取得对象，本地宿主从 runtime.application.workspace(id) 取得相同接点，无需捕获内核 Scope 或另建注册表。局部关系继续使用 createResource/createReaction 或局部 hooks；它们各自保留明确的 owner。

工作区登记删除或宿主退出会结束该工作区的资源与持续关系。资源先交付 owner 失效，排队反应停止，已受理的业务函数仍沿原 owner 完成；新登记产生新身份，旧对象不复活。普通 workspace.share(source$) 保留有限流完成语义；共享 ResourceSource 使用 workspace.shareResource(source)，由同一原观察流和工作区读取寿命决定有效性。

workspace.shareResource 接受资源对象或其 observations$，同一个原流复用同一 reader。构造不连接，不重新定义合同或解析已知领域值；普通函数、Promise、Observable 与 hooks 共用该对象。工作区退出向仍活跃的外部消费者交付原原因的 error 观察，晚读不能取得旧 ready，不需要取得 Workspace 的内部 Scope。

来源释放失败与业务读取错误分别负责。workspace.shareResource 将真实来源的清理失败交给工作区退出链汇总，资源仍交付原 owner 撤销原因。直接构造 shareResource 或 ResourceReader 时，可用 onCleanupError 接收来源的释放失败；它与 onObserverError 区分，不接收消费者回调异常。没有该接点的显式退订同步抛出清理失败；AbortSignal 或 read 内部释放没有退订调用者，报告 reactive.resource.cleanup.failed 并保留原读取结算。

Workspace、Conversation、Value/Cell、ResourceValue、资源实例与反应绑定不提供所属 Scope。借用者释放自己的订阅或取消一次 read，停止自己启动的 ReactionRun；创建 Scope 的 owner 才负责销毁共享关系。普通应用继续借用 Workspace 和组件 hooks，无需反向取得内部 Scope。内核装配显式持有 Scope，并通过 createWorkspace(scope, identity) 与 Conversation 的首个构造参数传入；公开对象不返回这份销毁权。

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

资源 owner 退出时，仍在观察的 observations$ 先交付 error Observation，其 error 是原 signal.reason，再正常完成；迟到订阅只取得撤销观察，不回放旧值或重连。values$ 在自身 owner 退出时正常完成，活跃期间的查询错误仍沿值流报错。借入资源的 useResource、Context 和公开资源消费同一失效，不单独保存旧 ready。普通有限 Observable 的最后值语义保留；跨 owner 借用需要明确的资源对象或 shareResource({ signal })，不要从裸流完成推测 owner。

`scope.dispose()` 同步撤销并清理；`scope.closed` 等待该 Scope 实际持有的已开始 Promise、异步清理及子 Scope，清理错误可使其拒绝。手动释放已开始的异步清理，即使子 Scope 已撤下，父 Scope 仍等待它。Reaction 失败由 `run.settled` 以 failed 结果结算，不自动使 `closed` 拒绝；关闭观察不取消业务，也不等待独立业务 owner 的整个 Operation。

父 Scope 撤销立即传播到所有子 Scope，后代不能在同步清理重入中读取旧事实或启动 effect；实际清理仍由 Scope 树汇总，子 signal.reason 保留父的原原因。

跨 Scope 的 computed 同时依赖实际读取的值与来源的读取资格。来源退出后，value/get/peek/read 拒绝旧缓存；peek 不跟踪业务值变化。`changes$` 按 RxJS 合同以 error 终止，分支恢复后重新订阅；`subscribe(accept, onError?)` 通过错误回调报告依赖失败并继续观察条件分支的恢复，省略时使用 owner 的 observerError。Resource 的 pending/error/ready 继续沿同一个 Observation 表达。

一次读取的 owner 使用 `read(params, { signal })` 加 `invalidations(params, context)`；推送 owner 使用 `observe` 发出完整 Observation。失效通知只表示需重新读取，不携带替代事实。写入成功后可显式 `instance.invalidate()`；这不会重发写入或唤醒模型。

公开资源槽的 refinement 或 codec 验证针对每一次 ready。某次值不能编码时，该次公开 Observation 为 error，原来源和同一出口继续连接；后续有效 ready 直接恢复，业务无需更换资源对象或重挂模块。真正的来源 error、complete 与 owner 退出仍保留各自终止语义，不自动重试来源。

## 会话文件使用同一资源

Program conversation、本地 SDK、远程 SDK 与可选原生 UI 均提供 `files: SessionFiles`。`const page = session.files.text(path, { offset, limit })` 与 metadata(path)、directory(path) 固定参数并复用对象；read、peek、values$、observations$、invalidate、useResource 和组合算子消费同一读侧。构造不读取；首次消费者等待正式 watch ready，变化先失效再重读，最后消费者退出释放共享监听。业务知道写入成功时可以显式 invalidate，不重发写操作。

单次 readText/readBytes/stat/list 仍读权威服务；raw watch 的 dispose/closed 反映本实现实际持有的观察。范围来自 exact 会话 cwd，baseFile 同样核验。readBytes 每次返回独立缓冲区，不建共享字节缓存。远程断线撤下旧 ready，新连接正式 ready 后重读，不承诺补齐断线事件或远端 unwatch ACK。Host files 选项必须借入已安装能力；不在 hook 中安装服务，不为文件建立第二份生命周期或状态。

文件或缺失文件通过包含目录的唯一正式 WorkspaceFiles.changes 流观察命名空间失效，覆盖目标创建、删除和替换；目录目标仍直通原流。连接时包含目录必须存在且为目录，否则报 `REACTIVE_NATIVE_FILE_WATCH_DIRECTORY_REQUIRED`；目录资格退出时来源报错，创建或恢复后须显式 invalidate 重新取得目录流，不自动搜索祖先、重绑或轮询。目录 ready 后通过公开 FS resolve/contains/stat 建立目标基线，每个目录帧重新核 scope、权限和路径并读取目标 opaque version 或 absent，不按目录 version 过滤。每个目录 change 帧都交付真实回读的目标帧，允许重复；兄弟文件活动也可能引起额外 stat 与资源重读；这是当前 FS 状态投影，不承诺所有瞬时状态或无损事件。它沿同一 owner 持有单个 watch，closed 等待唯一 generator 实际 unwatch。

ready 是最近一次读取切面，不是永久新鲜保证。单父目录的原生通知可能合并或缺失，Linux 上裸 writeFile 恢复也可能没有通知，已知裸写入后须显式 invalidate。固定 rc.3 本地 FS 的同路径 change 在 50ms 内可能被抑制且没有尾通知；已知连续写入完成后应显式 invalidate，需要立刻确认则调用 readText/stat 等权威查询。不加轮询、固定延迟或自动 retry 来伪装原生通知保证。

## 派生仍使用标准 Observable

已有 Promise API 使用 `abortable(signal => read(signal), ownerSignal)` 接入 RxJS，也可用 `firstValueFrom` 取得同一次等待的 Promise。外部取消以原 reason 结束等待，已经取消时不进入 read；退订只撤销本订阅，迟到结果和拒绝不再交付。它不承担底层提供者的清理等待，也不取消其他消费者或已受理业务。

`ResourceSource<T>` 是 `Observable<Observation<T>>` 或提供这个 observations$ 的对象；不要求对象另有 read、peek 或 subscribe。正式资源、SDK 资源和 useResource 返回的轻量对象都能直接组合。`combineResources({ stock, quote })` 可以混合对象与观察流，只有所有依赖 ready 才产生派生 ready。对象和它的 observations$ 使用同一个来源与原 owner，不调用 read/peek、不重复解析或额外持有连接。`mapResource(project)` 只接受同步投影，保留 pending/error，并将投影异常或 thenable 返回表示为该代 error；异步读取使用 switchResource 选择 observeResource 的查询，不交付 ready<Promise>。

需要把完整派生关系作为可读取对象传给普通函数、Promise 或其他模块时，用 `shareResource(source, { signal: scope.signal })`；source 可以直接是资源对象或 combineResources 返回的观察流，options 也可省略。read、peek、subscribe、values$、observations$ 共用 RxJS 的唯一当前快照；已经解析的领域值不再重复 schema parse，不需要另建 Scope、合同或绑定表。构造不连接，有限来源的最终观察保留至最后消费者退出，之后显式读取再连接；显式 signal 固定读侧寿命，无显式 owner 时遵循 refCount 和来源有效性，取消不重新执行任何命令。

`workspace.share(source$)` 则由工作区持续持有来源连接，同一来源跨会话共享，消费者全部退订也不停止来源。构造共享连接会立即订阅，应放在 defer 等订阅边界，不在 render 中执行。有限来源的最后值和错误仅在工作区 owner 活跃时重放；owner 退出后已有观察完成，之前捕获的流在迟到订阅时直接完成，不交付旧值或旧错误，也不重新连接来源。业务结果需要跨 owner 留存时，由业务自身的持久事实负责。

切换查询使用 `source$.pipe(switchResource(project, { dependencies: value => [value.sku, value.revision] }))`，project 可以直接返回资源对象或观察流。只选真实影响读取的字段，元素按 Object.is 比较；框架捕获依赖 tuple，并保留连续相同有效依赖的连接。pending/error 会撤旧读取，同参数恢复 ready 也重新读取。退出派生只归还这次订阅，其他读者和原业务执行仍归各自 owner。不要手写 JSON.stringify、Observation 比较器或额外 query cache。

后台组件从 `@hanger-source/dsh-reactive/program` 调用 `useResource(source)`，直接读取 ResourceSource；`useResource(factory, deps)` 也可以选择资源对象或构造观察流。`useResource(implementation, params)` 按当前程序 Scope 共享同一实现与参数；跨会话共享时显式把绑定放到适当共享 Scope，再把实例传入。资源对象及 hook 返回对象可直接交给 `Context source={source}` 和 resourceView 对应的 Expose 槽，不需要先取 observations$。

已有普通 Observable 直接交给 `useShared(source$)`、`useObservable(source$)` 或 `useRelation(source$)`；确实构造管道时再使用 factory 与 dependencies。它们保留流元素的原类型，不根据业务值的 status 字段推断资源，也不把 error Observation 当成 RxJS error。

`useShared` 与观察流形式的 `useResource` 由 RxJS 持有唯一共享连接及重放值。来源完成或报错后，可在 Program 活跃期间按原 source identity 读取终止事实；Program 失败或退出会停止既有观察并清空该缓存。之前捕获的流在迟到订阅、包括完成清理期间的重入订阅时直接完成，不重放旧值或错误，也不重新打开尚未读取的来源。直接借入资源对象的 observations$ 则保留原来源及原 owner；Program 退出不销毁工作区或业务系统拥有的事实。

组件自身的持续关系用 `useScope(setup, dependencies)`：commit effect 建立子 Scope，setup 同步执行，可返回同步或异步清理函数；卸载或依赖变化立即停止关系，StrictMode 重新 setup 使用新 Scope，未提交的 render 不启动关系。在 setup 中可用 `createReaction(scope, options)` 并显式 `start()`，与资源、标准 RxJS 和业务 hook 组合。该 Scope 只拥有这一关系，不拥有共享 Program 或 Workspace。

需要普通函数读取、写后失效或将对象交给其他业务 hook 时，用 `useResourceInstance(implementation, params)` 取得相同实例，再按需 `useResource(instance)`。取得实例不启动读取；不要为 read、Context 和公开 View 各建一个资源。

局部 hook 可以直接 `useResourceInstance(() => ({ value: z.string(), read: () => target }), [target])`；observe/invalidations 与 createResource 使用同一选项和语义。局部反应直接 `useReaction(() => ({ event, result, observe, operator, execute }), dependencies)`，继续显式 start。factory 纯构造选项，dependencies 与 React useMemo 相同，必须列出捕获目标、数据源或业务函数的身份依赖；不要在 factory 中启动查询或动作。相同组件和依赖保留实例，其他组件不凭相同名称共享局部对象；共享合同仍用 implementation/params 入口。

局部对象由当前 Program 的响应式作用域持有，可把整个资源／反应组合从业务 hook 返回给其他模块、普通函数或公开 View。依赖改变后取得新对象，之前捕获的资源与显式反应运行保留原目标，不自动重定向或停止。组件退出只释放组件查询，Program 失败或退出才撤销这些对象的查询和感知；已受理业务保持原 owner。

## 反应是显式运行

可复用反应采用 `defineReaction({ name, params, event, result })`、`implementReaction`、`bindReaction`；局部采用 `createReaction(scope, options)`，name 可省略，多个局部对象仍各有自己的运行身份。两者都是 `instance.start()` 才执行。

`start({ scope })` 在 observe、operator、execute 之前登记双方，同一 Scope 去重，双方须可用，各自 `closed` 等待 run 真正 settled。不要在 start 后用 `scope.own` 补登记。stop 后仍 draining 时同实例再次 start 报 `REACTIVE_REACTION_ALREADY_RUNNING`；StrictMode 或依赖变化不自动重启、延后或复活。每个组件代际需要独立运行时，使用 `createReaction(scope, options).start()` 创建独立实例。execute 不得等待自己 owner 的 `closed`，否则与 owner 等待 run 形成自等。失败由 run.state 的 failed 和 resolve 的 settled 表达，不改变 Scope.closed 的失败语义。

程序内用 `useReaction(implementation, params)` 取得程序拥有的相同参数实例，不手工创建 Scope 或绑定表。hook 不自动 start；同实例的重复并行 start 会失败。默认运行由实例 Scope 持有；组件借用已有 Program 或 Workspace 实例时，在 `useScope` setup 中显式调用 `reaction.start({ scope })`，让组件 Scope 同时持有该 run，任一 owner 退出即停止；Program 失败或退出会释放查询、感知和排队工作，已受理业务仍归原 owner。Reaction 执行失败由 run 结算，只有业务明确传播时才进入 Program 失败。

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

`stop()` 同步停止来源、排队工作和 Observable 订阅；已经进入 execute 的裸 Promise 仍归本次 run 结算。存在未完成 Promise 时，state$ 交付 draining，settled 等实际结果解析及清理结束后再给终态；同实例在此之前不能重新 start。switchMap 失选的迟到成功不补发到 events$，迟到失败仍进入原 run；多个独立失败保全为 AggregateError。不要先 await settled 再释放业务执行正在等待的条件。

来源或执行结果同步完成时，订阅返回并接收清理函数后才交付终止；这次连接传播出的清理失败仍进入 failed，不回滚已经发生的业务。来源内部自行吞掉的错误不由框架恢复，不要通过修改 RxJS 全局错误配置改变其他模块的语义。

```ts
const run = reaction.start();
run.state$.subscribe(state => {
  if (state.status === 'failed') console.error('restock.reaction.failed', state.error);
});
```

取消监控订阅不停止运行。未处理错误终止这次运行，重新 start 是显式决定。stop 或 Scope 退出停止感知及尚未开始的 RxJS 工作，不取消已受理 Action/Job。跨重启执行、事务幂等和定时调度归原业务或原生 owner。

持续事件需要提交会话时，先用 `host.service(actorId).open(sessionId, Program)` 得到固定服务身份端口；调用其 `resolveAction` 或 `submit`，不能逐调用自报 actor 或继承人的 Goal。同一应用视图也可创建或恢复会话。
