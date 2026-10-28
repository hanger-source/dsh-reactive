---
"@hanger-source/dsh-reactive": major
"@hanger-source/dsh-reactive-client": major
"@hanger-source/dsh-reactive-skills": minor
---

显式owner的ResourceReader将来源正常完成与资源有效性结束分开：有限来源立即释放真实连接，资源观察和values$继续接受owner撤销，不让活跃的外部hook、Context或视图永久保留旧ready。空完成或pending完成交付EmptyError以结算读取，不重复查询或添加状态缓存。远程Invocation.activity仍与SDK共用同一资源，原生流在终态释放，业务完成改用result或按returned/failed筛选；不再用lastValueFrom(activity$)推断业务完成。未指定owner的本地不可变事实及普通Observable保留原来源的有限语义。更新本地/远程及浏览器直接消费者、原生UI试验和同版本开发指导，迁移见docs/migration-resource-completion.md。
