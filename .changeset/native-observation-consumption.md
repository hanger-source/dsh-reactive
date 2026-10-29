---
"@hanger-source/dsh-reactive-dsh": minor
"@hanger-source/dsh-reactive-skills": minor
---

Jobs 的状态、输出与会话列表沿同一原生注册表重新读取。输出追加更新 JobView 的输出元数据；显式 collect 与已安装 job_output 工具的实际消费使同源读取失效，不改变原生结果的单次交付或伪造 JobEvent。新增 job.invalidate，供宿主在直接调用缺少消费通知的原生注册表后显式重读。

Team 通过正式投影变更通知观察任务板、成员与投影失败，不再自行筛选日志类型。waitForChange 的消费者等待在取消或来源退出后及时结束；原生先结算、同轮随后退出时也不交付旧资格的成功。

开发 Skill、架构说明及仓外原生能力消费者同步这些来源与生命周期边界。仅记录待发变化，不按建设批次升号或执行发布。
