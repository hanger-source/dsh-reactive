---
"@hanger-source/dsh-reactive": minor
"@hanger-source/dsh-reactive-dsh": minor
"@hanger-source/dsh-reactive-client": minor
"@hanger-source/dsh-reactive-remote": minor
"@hanger-source/dsh-reactive-skills": minor
---

公开请求声明、真实输入、独立调用与回复发生身份，以及当前 Goal 的权威输入关联。请求的工具和最后回复由同一原生投影事实派生，未取得步骤证据的外部回复不归入业务请求。

工具发生以正式 start 序号区分，保留跨轮复用 provider callId 的完整历史。人工问题只关联唯一真实的同 ID 提问，后来的普通工具不改变旧问题来源；rc.3 无法核验重复提问代际时明确拒绝来源读取或人工参与，不猜测原工作。

新增可提取的业务范围选择器，按应用定义的工作身份共同观察多个已取得的会话。保留完整多输入、原会话、问题、等待和 Goal 修订，分别呈现进展与关联条件，不产生新的继续权限或任务状态。

本地和远程消费者共用原始因果事件，纯范围观察不执行业务输入 transform。远程会话保留真实父会话身份。共享派生读侧允许沿来源和观察者连接寿命使用，无须构造额外 owner。
