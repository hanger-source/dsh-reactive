---
"@hanger-source/dsh-reactive": minor
"@hanger-source/dsh-reactive-skills": patch
---

工作观察直接消费动态会话来源资源，成员增删、重排与失效共同更新资源、事件和进展帧，并保留未变来源的实际连接。WorkHandle.sessions 统一为成员 ResourceReader；捕获成员仍借用原来源，不因名单退出销毁会话或转向同名新来源。提供对应消费与迁移指导。
