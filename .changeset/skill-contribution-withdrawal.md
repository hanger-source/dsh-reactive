---
"@hanger-source/dsh-reactive-dsh": patch
---

Program 的最后一项 Skill 贡献退出时立即撤销对应原生目录 provider 与 named prompt，无需等待下一次模型准备。相同目录或名称仍有贡献时保留原注册；退出继续加入原 Fiber 的异步清理，named prompt 释放失败由准备与 owner 关闭如实报告。
