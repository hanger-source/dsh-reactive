---
"@hanger-source/dsh-reactive": patch
"@hanger-source/dsh-reactive-skills": patch
---

useShared 与 factory 形式的 useResource 将共享连接和终止缓存绑定到 Program 寿命。Program 失败或退出后，既有观察完成，之前捕获的流不再重放旧值或错误，也不重新打开尚未读取的查询；完成清理期间的重入订阅遵循同一边界。直接借入的 ResourceReadable 保留原对象与原 owner。
