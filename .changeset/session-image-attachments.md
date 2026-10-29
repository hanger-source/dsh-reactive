---
"@hanger-source/dsh-reactive": minor
"@hanger-source/dsh-reactive-dsh": minor
"@hanger-source/dsh-reactive-client": minor
"@hanger-source/dsh-reactive-remote": minor
"@hanger-source/dsh-reactive-native-ui": minor
"@hanger-source/dsh-reactive-skills": minor
---

提供 SessionAttachments.readImage，使 Program、本地与远程 SDK 和可选原生 UI 按原会话资格读取已登记图片。复用原生 Controller 的引用授权与冷读取，字节采用独占缓冲区和正式 binary codec；取消只结束消费者等待，原生 owner 退出仍等待实际读取结算。

历史文件附件仍缺少 dsh rc.3 的公开会话授权读取入口，不以内容哈希或上传回执扩大读取权限。
