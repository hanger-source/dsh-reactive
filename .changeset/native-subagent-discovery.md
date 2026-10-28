---
"@hanger-source/dsh-reactive-dsh": minor
"@hanger-source/dsh-reactive-skills": minor
---

新增/subagents公共子入口，以createNativeSubagents接入宿主已有的正式子Agent服务。forSession(source).catalog提供同源资源，保留one-shot、continuable和external持久目录身份；readDescendants使用原生递归查询并保留分支诊断。父会话目录及实际Agent代际变化撤下旧有效性，冷读不加载Agent、创建Goal或投递消息；读取取消立即结算，SDK退出不停止原生子执行。NativeChildWork继续专用于带独立Goal的子程序委托，不把普通发现转换为Operation或复制执行引擎。公共exports、仓外消费者和同版本开发指导同步覆盖该入口。
