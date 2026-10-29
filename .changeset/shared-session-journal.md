---
"@hanger-source/dsh-reactive": minor
"@hanger-source/dsh-reactive-dsh": minor
"@hanger-source/dsh-reactive-client": minor
"@hanger-source/dsh-reactive-remote": minor
"@hanger-source/dsh-reactive-skills": minor
---

本地、远程及 Program 会话共用 journal 的原生消息分页、冷安全开场和持久追加观察。Host 借用确切 Context 中已安装的 Controller/Query 并拥有读侧的取消与实际清理；子会话使用已核验的原生父子地址，冷会话恢复沿原生日志接合追加顺序。Gateway 转发同一入口，独立 Runtime 不隐装 Controller。远程 openEvents 继续由官方 RemoteJournalStream 管理重连和缺口，新增单次开场取消，并保留读取与清理同时失败的完整原因；同一会话资格撤销只向高级流交付一次原原因。
