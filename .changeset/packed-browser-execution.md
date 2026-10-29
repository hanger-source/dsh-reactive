---
"@hanger-source/dsh-reactive-native-ui": patch
"@hanger-source/dsh-reactive-remote": patch
---

日常实包消费与正式发行验收使用同一 Playwright 浏览器流程，实际执行 SDK、原生 Slot 与请求进展页面。认证 Host 从独立后台实包安装启动，原生 UI 继续使用独立 React 18 安装与构建产物。成功回执在异步退出完成后生成，发行证据包含真实页面结果；仅有 bundle 构建不再满足浏览器验收。
