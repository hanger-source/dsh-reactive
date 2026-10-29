---
"@hanger-source/dsh-reactive": patch
"@hanger-source/dsh-reactive-dsh": patch
"@hanger-source/dsh-reactive-client": patch
"@hanger-source/dsh-reactive-remote": patch
"@hanger-source/dsh-reactive-native-ui": patch
"@hanger-source/dsh-reactive-skills": patch
---

将工具的 Goal 控制关联固定到真正准备并准入的模型步骤，完成、等待与 Operation Follow 共同保留原修订。修复暂停目标后的独立问题仍可完成该目标，以及旧模型响应在 Goal 编辑后借用新修订的问题；PTC 子调用沿根步骤使用同一关联。

结果续接在原生 prompt 装配之前完成准入和 rearm，异步等待后按本次实际准备事实复核，避免旧 prompt 与新 Goal 身份混用。无 Goal 的结果在连续模型步骤中保留原关联，不能借用会话中另一个 Goal；真实原生 Goal 控制仍可为后续步骤建立新关联。业务执行、原生 Goal 控制与读取的 owner 保持各自职责。

正式工具 dispatch 结束后，后台代码继承的 Agent 身份不再签发 Goal 控制；同步重入通知不能覆盖较新关联。结果 rearm 后拒准入时撤下本次尚未消费的自动推进，原消息与原关联保留，供新的可信输入处理。
