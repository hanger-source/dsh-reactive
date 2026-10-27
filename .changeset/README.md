# 版本与变更说明

六个公开包由 Changesets 的 fixed 组一起计算版本和内部依赖。根目录是 private workspace，正式包位于 `packages/`。

用 `npm run changeset` 记录用户可感知的变化，或者用标准 CLI 的 `--minor`、`--patch`、`--major` 与 `--message`。Changesets 负责 SemVer、预发布递增和各包 CHANGELOG；不要直接编辑六包版本来绕过变更计划。

`npm run release -- version --channel next` 进入正式 `next` 预发布模式并应用版本。`--channel stable` 使用 Changesets 的 `pre exit` 退出预发布。版本和锁的 diff 需要检查、验证和提交，脚本不会自动 commit、push 或 publish。

完整产物准备、源码追溯、候选安装、指定 registry 发布与重新安装见 [发行流程](../docs/releasing.md)。
