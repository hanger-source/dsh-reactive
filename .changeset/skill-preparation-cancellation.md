---
"@hanger-source/dsh-reactive-dsh": patch
"@hanger-source/dsh-reactive-skills": patch
---

Skill 步骤准备在声明、原生 catalog 或本会话工作目录变化时取消旧查询并重新准备，避免不配合取消的 provider 阻塞当前集合。共享 provider 和其他会话保持原有寿命，迟到正文不写回，当前读取错误继续传播。
