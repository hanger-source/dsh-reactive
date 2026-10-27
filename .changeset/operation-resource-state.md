---
"@hanger-source/dsh-reactive": minor
"@hanger-source/dsh-reactive-native-ui": patch
"@hanger-source/dsh-reactive-skills": patch
---

shareResource 将已有或组合后的 Observation 流交付为共享只读资源，普通函数、Promise、RxJS 与 hooks 复用同一快照和读取寿命。OperationHandle.state 复用该入口，当前状态读取与 settled 终态等待保持独立，本地和远程操作及可选原生 UI 不再另存观察状态。操作来源结束仍没有业务终态时明确进入查询错误，不永久等待；已有最终观察保留至最后消费者退出，之后显式读取重新查询原操作，不再次执行 Action 或登记 follow。
