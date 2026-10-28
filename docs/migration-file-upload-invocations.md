# 上传调用与回执等待

本地 Program Session、远程 SDK 和原生 UI 的 `uploadFile` 同步返回 `FileUploadInvocation`。调用对象不提供 thenable；直接 `await session.uploadFile(...)` 得到的是调用对象，不会等待上传回执。保存调用对象，再读取 result：

```ts
const call = session.uploadFile(bytesOrBlob, { name: 'order-details.pdf' });
const uploaded = await call.result;
await session.submit({
  requestId: 'order-details', intent: 'followup', delivery: 'inject',
  scope: { orderId },
  content: [{ type: 'file', receiptId: uploaded.receiptId }],
});
```

调用前已能确定的句柄退出、身份或权限无效，以及预取消的方法 signal，由 preflight 同步抛出，不产生调用对象。取得 call 后，字节读取失败、准备期间的取消和实际上传拒绝沿 result 结算；远端权限仍由 Gateway 判断，拒绝保留在 result 中，不能假定所有失败都会同步抛出。

result 是同一次上传的 `Promise<FileUploadValue>`，保留真实回执或错误。上传本身不提交模型输入；receipt 仍绑定实际接收上传的原生 Session 实例，未提交时不能跨 Agent 关闭或进程重启存活。

需要结束本消费者的等待时使用 wait：

```ts
const call = session.uploadFile(bytesOrBlob, { name: 'order-details.pdf' });
const uploaded = await call.wait({ signal });
```

wait 观察同一个 result，取消它不重新执行或撤回上传。方法 options.signal 与 wait 的 signal 有不同责任：方法信号约束字节准备和分派前检查，也作为 wait 的默认信号；单独传给 wait 的信号只约束这次等待。Uint8Array 在调用时复制，调用后修改原数组不改变上传。Blob 异步读取被取消后，迟到字节不得再激活会话或发出 HTTP。Session 或原生 UI 退出也可阻止尚未分派的准备；分派后，方法信号、Session release 和 UI dispose 只结束 wait，不改写 result。

所属 SDK 或 Connection 物理退出可能让远程 result 拒绝为 `@hanger-source/dsh-reactive-client` 导出的 `FileUploadReceiptUnknownError`。该错误保留 sessionId 和 cause，表示没有取得正式回执，不证明服务器没有文件。调用 id 不能找回未知 receipt，框架不自动重传。已知的准入、格式或存储拒绝仍保留对应错误；调用失败本身不证明没有发生存储副作用。

call.activity 是同源有限 `ResourceReader<InvocationActivity<never>>`，activity$ 引用它的 values$。读取、订阅或交给 hooks 都消费同一次调用：

```ts
const current = await call.activity.read();
const snapshot = call.activity.peek();
const activity$ = call.activity$;
```

read 读取当前事实，不等待上传完成。pending 表达准备中的调用，running 只表示已经分派，不承诺 Gateway 或存储受理。returned 表达 result 成功，failed 以 ready.value 保存真实错误和阶段。终态不会随 Session 或 UI 退出改写，晚读、晚订阅仍可取得同一次事实；取得完整回执继续使用 result 或 wait。

调用来源为 `{ kind: 'file-upload', sessionId }`，表达本次 Session 上传，不继承 Action 或 Goal。id 只标识本次内存调用，不是 receipt、持久恢复引用或幂等键。Gateway wire 继续只返回 `FileUploadValue`，调用对象及其活动由本地调用方或 SDK 持有，不新增服务端 Invocation 查询入口。

消费不同来源的通用活动时保留 Invocation 的第三个 Source 泛型：

```ts
import type { Invocation, Json } from '@hanger-source/dsh-reactive';

function activity<T, O extends Json, Source>(call: Invocation<T, O, Source>) {
  return call.activity;
}
```

原生 UI 的 uploadFile 保留 SDK 原调用对象，native.invocation 接受上传来源并借入同一活动：

```ts
const call = native.uploadFile(bytesOrBlob, { name: 'order-details.pdf' });
const activity = native.invocation(call);
const uploaded = await call.result;
```

UI 观察仍随原 UI generation 退出；退出 UI 观察不改变已分派上传的调用事实或回执。
