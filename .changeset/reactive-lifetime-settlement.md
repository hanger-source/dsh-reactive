---
"@hanger-source/dsh-reactive": minor
"@hanger-source/dsh-reactive-dsh": minor
"@hanger-source/dsh-reactive-native-ui": minor
"@hanger-source/dsh-reactive-skills": minor
---

将跨作用域派生的读取资格接入同一 signals 图；读取失败通过订阅错误接点传播到普通消费者和原生 UI。

Reaction 停止立即撤销来源与排队工作，draining 和 settled 保留已开始 Promise 的实际结算与迟到失败；运行实例结算前不接受第二次 start。

Action 和 NativeTools 的准入等待随原调用、声明代际与 Program 退出及时结束。原 execution 身份保持不变，原生最终 guard 拒绝已撤销的旧准入；已经进入业务的工作保留原 owner。
