# 共享 Program 合同

远程 SDK 工厂和 `retainNativeSession` 要求由 `defineProgram` 签发的 `SharedProgramContract`。在宿主与客户端共同导入的纯合同文件中明确声明名称和合同版本：

```ts
import { defineProgram, defineSurface, resourceView, z } from '@hanger-source/dsh-reactive';

export const InventoryProgram = defineProgram({
  name: 'inventory',
  version: 1,
  input: z.object({ sku: z.string() }).strict(),
  surface: defineSurface({
    views: { revision: resourceView(z.number()) },
    actions: {},
  }),
});
```

name 须满足 `[a-z][a-z0-9_]{0,79}`。version 为正安全整数，省略时为 1；它是发布者声明的 Program 合同版本，与 npm 包版本和 Environment id 无关。输入、公开 Surface、codec 或业务语义发生不兼容变化时，发布者增加 version，并部署对应宿主和消费者。名称与版本一致不证明 schema、函数或 codec 等价。

## 构造与借用入口

`createProgramClient`、`bindProgramClient`、`createServiceProgramClient`、`bindServiceProgramClient` 与自行创建 SDK 的 `retainNativeSession` 要求共享定义。无名 Program 或手工拼装的同形对象不能进入这些构造入口。

```ts
import { createProgramClient } from '@hanger-source/dsh-reactive-client';

const client = await createProgramClient(InventoryProgram, transport);
const session = await client.open(sessionId);
```

只在本地使用的 `defineProgram({ input, surface })` 保留原入口。模块只定义自己的 Surface，不为了组合增加 Program 名称。`ProgramSessionClient<P>`、`RemoteProgramSession<P>`、`NativeSession<P>` 和借用已有对象的 `bindNativeSession` 保留 `P extends ProgramContract`；通用消费者无需取得或销毁 SDK。

## 身份核验

SDK 随 Program 请求发送预期 name/version，Gateway 核对实际环境或会话的定义。create、completeCreation 与 resume 先核合同再进入创建、补齐或恢复；open 和后续会话请求也核对实际目标。SDK 在建立句柄时复核取得回执的身份，随后按已核合同解码。createWorkspace、describe 继续表达应用级工作区登记与发现，不绑定某个 Program。

自定义传输沿 `ProgramAddress.program` 携带这份身份；创建和补齐请求也各自包含 program。View、人工参与、进展、journal 与 Operation 的物理请求及重连都保留原 SDK 捕获的身份，不根据新 baseline 猜测合同。已受理 Action 的查询核对原调用记录的 Program，后续会话变化不撤销它的业务结果。

`programIdentity(program)` 取得不可变的 `ProgramContractIdentity`，`assertProgramIdentity(expected, actual)` 在不匹配时抛出带 expected/actual 的 `ProgramContractMismatchError`。宿主确定拒绝使用 `reactive/program-contract-mismatch`；目标仅有本地无名定义时使用 `reactive/shared-program-required`。本地取得传入实际 Program 定义继续核 exact 对象；另一份同名同版本定义不能替代它。

create 或 completeCreation 收到形状有效但 Program 身份矛盾的成功回执时，SDK 抛出 `CreationUnknownError`，保留本次准确 reference 与原 cause，不建立句柄。创建可能已经完成；调用方可以沿该引用显式补齐，SDK 不自动重建。成功值若先被正式 codec 拒绝，则沿其协议 failure；不能从 `gateway/internal` 推断尚未创建。

合同选择不授予 endpoint、actor、Workspace、Session 或业务权限，也不是幂等键。发生身份拒绝时应使用目标实际发布的合同；不要换名称、猜版本或忽略核验后继续解码。

`bindNativeSession` 仍核验 SDK 的真实定义对象、Connection 与 exact Session generation；共享 name/version 不能代替这些进程内归属。`retainNativeSession` 在取得 SDK 或原生引用前验证共享定义，失败仍沿原 owner 释放已取得资源。
