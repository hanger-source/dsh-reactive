---
"@hanger-source/dsh-reactive": minor
"@hanger-source/dsh-reactive-dsh": patch
"@hanger-source/dsh-reactive-skills": minor
---

模块通过 useWorkspace 和 useProgramSession 借用真实所属环境，无需逐层转交会话与工作区。类型化输入和提交须核验当前 Program 合同，普通输入保持原服务参与语义。

OperationFollow 的结果观察与恢复筛选共同核验正式结果、原 continuation 与完整 wait；同名无关请求不再冒认原操作反馈或阻止恢复。
