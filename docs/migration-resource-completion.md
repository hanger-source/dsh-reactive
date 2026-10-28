# 来源完成与资源有效性

显式 owner 的 ResourceReader 在来源正常有限完成后保留有效性观察，直到 owner 撤销。底层来源仍按本来时点释放；框架不延长 I/O、不重复查询、不增加缓存。资源的 observations$ 与 values$ 不再因为这个来源完成而结束，owner 退出会撤销旧 ready，再完成现有观察。

曾经依赖有限来源完成而关闭资源订阅的消费者，应改为由资源本身保留读取 owner；仍活跃的外部 hook、Context 或视图才能在原 owner 退出时撤销最后 ready。所有消费方式继续使用同一对象，不为 hooks 建立另一份生命周期。

## 调用完成

远程 Invocation 的原生观察在 returned/failed 后释放，activity 资源仍由 SDK 拥有。等待正式业务结果使用 result；等待观察显示终态时显式选择业务状态：

```ts
const receipt = await invocation.result;
const terminal = await firstValueFrom(invocation.activity$.pipe(
  filter(value => value.state === 'returned' || value.state === 'failed'),
));
```

不要用 lastValueFrom(invocation.activity$) 推断业务完成，它现在会继续等待读取 owner。activity.read 仍只读取当前事实，result 与活动帧沿各自入口到达；拿到 result 不代表活动流已经更新到 returned。业务 failed 是 ready.value 中的调用事实，SDK 撤销是资源 error，两者保持区分。

没有额外读取 owner 的本地不可变 Invocation 事实，继续沿原来源有限完成。普通 Observable 的完成语义不变。跨 owner 借用需要持续有效性时，提供正式资源对象及明确 owner，不能把普通有限值流当作长期资格。

## 空来源与退出

显式 owner 的来源空完成，或最后只交付 pending 后完成，会交付 EmptyError 观察，结算 read 等待。owner 退出仍以原 signal.reason 撤销资源，值流在自身 owner 退出时正常完成；peek、read 和回调式 subscribe 拒绝已经结束的 owner。已退出 owner 的迟到观察不回放旧业务终态、不重新打开查询。

Workspace 的资源和反应使用 workspace.resource、workspace.reaction 或本地 application.workspace(id) 取得同一编程对象。工作区登记删除后，这些资源与持续关系遵循相同撤销语义；已受理业务继续由原 owner 完成。
