export interface DeveloperSkill {
  readonly name: 'dsh-reactive';
  /** 与这份 Skill 配套的六包固定版本。 */
  readonly version: string;
  readonly directory: string;
  readonly entrypoint: string;
}
/** 返回当前安装包内的 Skill；不会安装或注册到全局 Agent 目录。 */
export declare function getDeveloperSkill(): DeveloperSkill;
