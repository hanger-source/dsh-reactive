---
"@hanger-source/dsh-reactive": minor
---

人工问题公开观察增加 sourceConflicts，拒绝把同 callId 的其他工具结果当作人工答案；问题句柄固定原 session 与调用序号，异步响应准入和远程控制均复核实际来源。Work 与可选原生 UI 共享冲突诊断，健康问题独立运行。远程 human 帧必须包含 sourceConflicts；自建协议消费者需同步该字段并处理 HumanQuestionSourceConflictError。该诊断不恢复 rc.3 已撤下的问题。
