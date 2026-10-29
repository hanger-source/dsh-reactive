---
"@hanger-source/dsh-reactive": patch
"@hanger-source/dsh-reactive-skills": patch
---

资源观察边界明确拒绝错误接入的裸值和缺字段帧，避免组合器产出缺少必需依赖的 ready。合法 Observation 保留原事实与订阅语义，显式 undefined 仍有效，后续合法帧能够恢复；该校验不解析业务值，也不支持将普通值流隐式转换为资源流。
