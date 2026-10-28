# 响应式读取与执行结算

## 来源作用域与派生值

computed 的依赖同时包含实际读取的业务值与来源 owner 的读取资格。来源退出后，即使消费者仍活着，value、get、peek、read 也不再返回旧缓存。peek 保留业务值不参与依赖跟踪的含义，读取资格仍会失效。

Value.changes$ 遵守标准 Observable 的 error 终止语义；条件切换使派生恢复后，重新订阅取得当前值。持续回调使用 subscribe(accept, onError?)，依赖失败经 onError 报告，之后可以观察条件分支恢复；省略 onError 时沿 owner 的 observerError 报告。资源的 pending/error/ready 仍由既有 Observation 表达。

自定义 NativeReadable 可实现可选的订阅错误回调。nativeSource 在值失效时通知原生消费者重新读取 getSnapshot，读取会抛出同一失败，不保留看似有效的旧快照。

## Reaction 停止与结算

stop() 立即停止感知、RxJS 排队项和 Observable 订阅。已开始的裸 Promise 无法由退订撤回，仍由原 run 持有到实际结算；存在这些工作时，状态为 draining，settled 尚未结束，同实例不能再次 start。

释放业务门闩或等待业务自行结束后，再 await run.settled。不要用 settled 作为放行业务的前提。Scope.dispose 仍同步发起停止，run 的结算事实在 owner 退出后继续有效。

events$ 保留 RxJS operator 选中的成功结果。switchMap 失选或 stop 后的迟到成功不补发；迟到拒绝、结果解析失败及清理失败仍进入原 run 的 failed 结算。单个失败保留原对象，多个独立失败聚合为 AggregateError。Observable 背后的外部副作用仍须由业务按其真实取消合同管理。

## 准入与业务执行

ActionBindingOptions.admit 的第四参数是本次准入的 signal；ProgramInstanceOptions.admit 的第五参数透传同一准入寿命。原始 execution 对象保持宿主签名，call.signal 继续代表原执行。直接绑定的 admissionSignal() 提供当前声明代际，active() 继续负责同步条件；框架不轮询普通变量。

绑定 release、声明退出、Program 失败或原调用取消可以结束未通过的准入。迟到检查结果不能重新开始业务。已经开始的可靠 Operation 准备要先结算其真实受理事实，再拒绝失效声明的业务执行；已经进入 execute 的业务不随声明退出撤销。

NativeTools 在 native 与 PTC 共用的正式工具链等待自有准入，并保留原调用的有限准入证明直到最终 guard。最后一份声明撤下后，即使以同一函数重新挂载，也不能复活旧准入。共享同政策的其他贡献仍在时，撤下一份不会取消有效准入。其他插件 gate 和原生审批仍由各自 owner 等待；本框架不接管它们的执行或取消合同。
