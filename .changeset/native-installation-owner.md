---
"@hanger-source/dsh-reactive-dsh": patch
"@hanger-source/dsh-reactive-skills": patch
---

修复原生依赖安装过程中宿主退出后仍启动后续安装的问题。应用装配、能力组合、扩展 setup、Runtime 启动及原生能力访问共同核验实际 Cordis owner 的完整祖先链；退出阻止新工作，已开始的安装和清理仍按原 owner 结算。
