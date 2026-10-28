---
"@hanger-source/dsh-reactive": minor
"@hanger-source/dsh-reactive-dsh": minor
"@hanger-source/dsh-reactive-client": minor
"@hanger-source/dsh-reactive-remote": minor
"@hanger-source/dsh-reactive-native-ui": minor
"@hanger-source/dsh-reactive-skills": minor
---

会话上传同步返回 FileUploadInvocation，普通 Promise、独立等待和有限活动资源消费同一次调用。字节准备可以取消；开始分派后退出等待不覆盖实际回执，也不重新上传。本地会话退出等待已经开始的原生存储，远程运输失败明确保留回执未知及原始原因。原生 UI、公开消费类型和版本化开发指导共同使用这份合同。原先直接 await uploadFile 的消费者需要改为读取调用的 result 或 wait()。
