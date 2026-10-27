import { readFileSync, accessSync, constants } from 'node:fs';
import { fileURLToPath } from 'node:url';

/** 只定位当前安装包；不安装 Skill、运行依赖或修改 Agent 配置。 */
export function getDeveloperSkill() {
  const metadata = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8'));
  if (metadata.name !== '@hanger-source/dsh-reactive-skills' || typeof metadata.version !== 'string' || !metadata.version) {
    throw new Error('REACTIVE_DEVELOPER_SKILL_METADATA_INVALID');
  }
  const directory = fileURLToPath(new URL('../skills/dsh-reactive/', import.meta.url));
  const entrypoint = fileURLToPath(new URL('../skills/dsh-reactive/SKILL.md', import.meta.url));
  accessSync(entrypoint, constants.R_OK);
  return Object.freeze({ name: 'dsh-reactive', version: metadata.version, directory, entrypoint });
}
