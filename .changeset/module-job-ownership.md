---
"@hanger-source/dsh-reactive": minor
"@hanger-source/dsh-reactive-dsh": minor
"@hanger-source/dsh-reactive-client": patch
"@hanger-source/dsh-reactive-native-ui": patch
"@hanger-source/dsh-reactive-skills": minor
---

动态集合合同只签发轻量地址，不永久缓存历史业务 key。Module 持有当前实例的动态声明名称：同地址重复渲染稳定，切换目标或撤下重挂产生新代际；已受理调用继续由原 owner 结算，SDK 仍按完整地址投射。

Job.wait 改为非消费状态等待，不再调用原生 registry.wait 或影响模型完成通知。显式 collect 消费原生输出 cursor，首个终态消费可取得 producer result；取消与清理不伪造结果确认。

公开 Disposable/AsyncDisposable 声明显式携带标准库依赖，浏览器不再依赖 Node ambient 补齐。仓外 UI 安装增加 ES2024/DOM、types:[] 的严格声明消费，日常与正式发行共用已有验证链。
