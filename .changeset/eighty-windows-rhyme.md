---
"@hanger-source/dsh-reactive": patch
---

修复同步 Effect 连续提交后重放历史声明帧的问题。框架只公开当前完整帧，避免已退出目标重新连接查询或暴露失效 Action；保留真实卸载清理、当前声明、发布重入与失败中止。
