---
"@hanger-source/dsh-reactive-dsh": patch
"@hanger-source/dsh-reactive-skills": patch
---

原生能力退出加入外层已经开始的 Fiber 清理，观察释放失败保留到实际资源静止后报告。Runtime、Application、Preset 与动态 Skill 在撤销前确定同一个退出回执，避免同步清理重入启动第二次退出；Skill provider 的退休等待真实 Fiber。应用安装沿同一正式插件就绪检查核验并回收失败范围。
