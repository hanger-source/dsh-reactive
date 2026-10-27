#!/usr/bin/env node
import { getDeveloperSkill } from './index.mjs';

const args = process.argv.slice(2);
if (args.length === 0) process.stdout.write(JSON.stringify(getDeveloperSkill(), null, 2) + '\n');
else if (args.length === 1 && args[0] === '--help') {
  process.stdout.write('dsh-reactive-skill\n返回当前安装包中开发 Skill 的路径和版本（JSON），不安装到全局目录。\n');
} else {
  process.stderr.write('不支持该参数。运行 dsh-reactive-skill 或 dsh-reactive-skill --help。\n');
  process.exitCode = 2;
}
