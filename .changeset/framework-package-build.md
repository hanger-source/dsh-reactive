---
"@hanger-source/dsh-reactive": patch
"@hanger-source/dsh-reactive-native-ui": patch
---

框架代码构建与打包资产准备分开，普通 core prepack、工作区打包和正式发行共享完整准备依赖。默认框架类型与行为检查独立于示例验证，保留现有发行资产。

Native UI 独立构建包含 client 浏览器产物前置任务。日常 tgz 与正式发行消费同一份安装包 Skill/CLI 检查，打包校验同时拒绝缺少 manifest 字面资产的产物。
