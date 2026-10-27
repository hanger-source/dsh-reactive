---
"@hanger-source/dsh-reactive-dsh": minor
---

原生 Approval 的会话对象提供同源 policy 资源，read、peek、观察、hooks 与跨能力组合共用正式会话政策，policy$ 直接使用其值流。会话切面读取保留 pending/error，原生代际和政策日志变化撤下旧 ready 并重新读取，错误可随正式通知恢复。冷读不恢复 Agent，政策读取不产生批准或 grant，审计事件与显式 setPolicy 继续由原生 owner 管理。仓外原生消费者与同版本开发指导同步覆盖政策组合。
