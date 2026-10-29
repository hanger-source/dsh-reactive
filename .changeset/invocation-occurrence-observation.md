---
"@hanger-source/dsh-reactive": minor
"@hanger-source/dsh-reactive-dsh": minor
"@hanger-source/dsh-reactive-client": minor
"@hanger-source/dsh-reactive-remote": minor
"@hanger-source/dsh-reactive-native-ui": minor
"@hanger-source/dsh-reactive-skills": minor
---

公开 ToolActivity.invocationId、tool.invocation 事实和 OperationFollowSnapshot.callSequence，将业务 Invocation、原生工具发生及原操作参与按正式身份连接。重复 provider 标签不再要求消费者解析不透明 ID，业务结果与原生后置结果分别保留。

完整 Invocation 在执行前登记来源关联，失败通过 registration 阶段结算且不进入业务或 Operation 准备。关系使用现有原生日志和投影恢复；缺证保留 null，不推测历史来源，也不改变既有 Source 或业务恢复合同。同版本开发指导说明固定 rc.3 下重复 PTC 子 ID 的正式冷读取限制。
