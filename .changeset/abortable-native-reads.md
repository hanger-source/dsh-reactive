---
"@hanger-source/dsh-reactive": minor
"@hanger-source/dsh-reactive-dsh": patch
"@hanger-source/dsh-reactive-client": patch
"@hanger-source/dsh-reactive-skills": minor
---

abortable 接受外部 AbortSignal，以原取消原因结束本次观察与 Promise 等待，忽略不配合取消的迟到结果。客户端等待复用同一互操作；Schedule 只读查询、冷 Team 投影和显式 Files 读取不再被迟到的原生回执阻塞。Files 保留实际 I/O 的退出等待；原生历史读取继续传递查询信号，不取消已受理业务或其他消费者。
