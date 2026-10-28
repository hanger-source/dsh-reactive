---
"@hanger-source/dsh-reactive": minor
"@hanger-source/dsh-reactive-dsh": patch
"@hanger-source/dsh-reactive-skills": minor
---

完整模块可以各自贡献相同的会话级 Skill 依赖与原生工具入口，无需把共同依赖搬到应用外围。Skill 按当前声明集合共享，规范化后的同目录只安装一次；只在最后一个贡献退出时撤下依赖，模块重排不重启同一准备。

原生工具保持其真实全局名称。相同工具仅在 enabled 相同且 admit 为同一函数引用时共享；不同参与条件或准入政策明确拒绝，不自动扩大、合并权限或重命名工具。异步准入后继续核对当前政策，移除一项相同贡献不撤销其他模块仍持有的入口。
