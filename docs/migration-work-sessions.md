# 工作会话观察迁移

`WorkHandle.sessions` 从数组改为 `ResourceReader<readonly WorkSessionHandle<K>[]>`。静态会话来源仍可直接传给 `observe(key, { sessions })`，无需修改输入；动态名单可以传入 `ResourceSource<readonly WorkSource[]>`。

原来直接索引句柄数组的调用需要先读取资源：

```ts
// 原调用
await work.sessions[i].state.read();

// 资源读取
const members = await work.sessions.read();
await members[i].state.read();
```

持续观察使用同一个 reader 的 `work.sessions.observations$`，也可以将 reader 交给已有资源 hook。名单 Observation 的 pending 表示名单暂不可用，error 表示读取失败；两者都不是空名单。`WorkSnapshot.sessions` 仍是普通数组，`select` 仍同步投影静态快照。

已取得的成员句柄始终指向原来源，即使名单移除该成员，或出现相同 sessionId 的新来源，也不会重定向。句柄借用来源和显式 signal；名单移除不销毁句柄或外部 Session，原来源退出时句柄失效。调用方负责取得和释放已授权 Session，Work 不转移其销毁权。
