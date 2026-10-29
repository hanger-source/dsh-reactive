# 人工问题的参与关联

`HumanQuestionOrigin.continuation` 将问题的真实调用、原输入与提问模型步骤的 Goal 关联放到同一份公开事实中。本地 SDK、远程 SDK、前台等待、迟答及冷恢复提供相同含义。

| 观察值 | 含义 |
| --- | --- |
| `continuation === null` | 缺少可信的原步骤关联；问题仍可观察，迟答不能单独取得继续权限 |
| `continuation.goal === null` | 原问题明确没有 Goal；回答仍按独立问题处理，不借用后来出现的目标 |
| `continuation.goal` 为 id/revision | 原问题属于该精确 Goal 修订；纯迟答只有在它仍 exact active 时才进入模型 |

响应者政策继续核验原问题、完整答案和真实响应者。关联是观察事实，不能通过复制、改写或提交该对象取得新的业务授权；回答资料问题也不等于批准操作。Action 的前台答案仍在原工具执行内结算，超时后的回答则是沿原问题反馈的原生输入，两者保留各自生命周期。

迟答已受理但原 Goal 暂停、替换或修订变化时，答案保留在原生队列。`answer()` 返回 true 只证明受理，不证明模型已经消费。新的真实要求可以与旧答案共同进入模型，权限仍由新要求和业务 owner 核验；回答本身不额外 resume Goal，也不解除 Operation 等待。宿主的 resume 只恢复会话，不等于恢复 Goal。

`WorkCondition` 增加 `question.reply-blocked`，提供原交付、`matchedInputs`、当前 Goal 和 unavailable、superseded、inactive 原因。它可与 `question.reply-pending` 同时存在。对 WorkCondition 使用穷尽分支的消费者应处理该条件，不再把“已回答”直接显示为“AI 已继续”。

手工构造 `HumanQuestionOrigin` 的测试或消费者必须明确填写 continuation。只有已知原问题没有 Goal 时才使用已知独立关联；没有证据时使用 null，不能拿当前 Goal 补齐。真实应用从公开问题对象取得该事实，通过 question.answer、answerQuestion 或前台等待入口参与，不自行登记问答或续接。

持久关联由正式工具 dispatch 在原生日志中记录，投影按实际 callSequence 恢复；读取旧问题不会创建关联、启动 Agent 或重放业务命令。没有该记录的历史问题保持缺证状态，不自动推测提问时的 Goal。
