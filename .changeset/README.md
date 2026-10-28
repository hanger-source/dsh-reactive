# 版本与变更说明

六个公开包由 Changesets 的 fixed 组一起计算版本和内部依赖。根目录是 private workspace，正式包位于 `packages/`。

用 `npm run changeset` 记录用户可感知的变化。当前处于 0.x 建设阶段：保持公共合同的缺陷修复使用 `--patch`，新增能力或需要消费者迁移的合同调整使用 `--minor`，并通过 `--message` 说明变化及升级影响。1.0 以公共合同、运行语义和发行体系的成熟度为依据。Changesets 负责 SemVer、预发布递增和各包 CHANGELOG；不要直接编辑六包版本来绕过变更计划。

`npm run release -- version --channel next` 进入正式 `next` 预发布模式并应用版本。`--channel stable` 使用 Changesets 的 `pre exit` 退出预发布。版本和锁的 diff 需要检查、验证和提交，脚本不会自动 commit、push 或 publish。

完整产物准备、源码追溯、候选安装、指定 registry 发布与重新安装见 [发行流程](../docs/releasing.md)。
