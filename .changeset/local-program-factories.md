---
"@hanger-source/dsh-reactive": minor
---

useResourceInstance 与 useReaction 支持局部选项 factory，由当前 Program 持有已有 Resource/Reaction 实例，React 依赖决定组件内身份。自定义业务 hook 可直接定义并返回完整资源与反应组合，无需另建参数合同或传入 Scope；构造不读取或执行，目标切换不重定向旧对象，显式运行、读取取消与已受理业务继续使用原 owner。共享 implementation/params 入口与 schema 输入输出类型保持原语义；同版本开发指导和仓外消费者同步覆盖局部组合。
