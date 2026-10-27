import { z } from 'zod';
import { bindAction, defineAction, implementAction, type ActionExecutionContext, type ActionImplementation, type BoundAction, type Invocation } from '@hanger-source/dsh-reactive';

// 合同可独立共享；输入保留 raw 方向，业务实现与展示消费各自的解析结果。
const revise = defineAction({ name: 'revise', description: '保存目标修订',
  scope: z.object({ merchantId: z.string(), revision: z.string().transform(Number) }).strict(),
  input: z.object({ revision: z.string().transform(Number) }).strict(),
  output: z.codec(z.iso.datetime(), z.date(), {
    decode: value => new Date(value), encode: value => value.toISOString(),
  }),
  observe: value => ({ savedAt: value.toISOString() }),
});
const implementation = implementAction(revise, {
  execute(input, call) {
    input.revision.toFixed();
    call.scope.revision.toFixed();
    call.scope.merchantId.toUpperCase();
    // @ts-expect-error 实现消费 input output，而不是原始字符串。
    input.revision.trim();
    // @ts-expect-error 目标是已解析的自有只读引用。
    call.scope.merchantId = 'another';
    return '2026-10-26T00:00:00.000Z';
  },
});
const typedImplementation: ActionImplementation<typeof revise> = implementation;
const bound = bindAction(typedImplementation, { merchantId: 'merchant-a', revision: '7' }, { admit() {} });
const typedBound: BoundAction<typeof revise> = bound;
const contract: typeof revise = bound.definition;
void typedBound;
void contract;

function consumer(execution: ActionExecutionContext) {
  const invocation: Invocation<Date, { savedAt: string }> = bound({ revision: '8' }, execution);
  invocation.result.then(value => value.toISOString());
  const observation: { savedAt: string } | undefined = bound.observe(new Date());
  void observation;
  // @ts-expect-error callable 使用 raw input 字符串，不使用实现收到的 number。
  bound({ revision: 8 }, execution);
  // @ts-expect-error 普通低层绑定必须明确传入可信执行关联。
  bound({ revision: '8' });
  const automatic = bindAction(implementation, { merchantId: 'merchant-a', revision: '7' }, {
    execution: () => execution, admit() {},
  });
  const automaticBound: BoundAction<typeof revise, true> = automatic;
  automaticBound({ revision: '8' });
  // 自动来源参数仍可接受 Surface 所要求的同一合同 callable。
  const exposed: BoundAction<typeof revise> = automaticBound;
  void exposed;
}
void consumer;

// @ts-expect-error 纯合同不再接受宿主 handler；不存在旧 defineAction 双轨。
defineAction({ name: 'mixed', description: '错误的混合定义', scope: z.null(), input: z.string(), output: z.number(), execute: () => 1 });
// @ts-expect-error 合同不是可执行实现。
bindAction(revise, { merchantId: 'merchant-a', revision: '7' }, { admit() {} });
// @ts-expect-error bind 目标仍使用 scope 的原始输入，不能反向扩宽合同。
bindAction(implementation, { merchantId: 'merchant-a', revision: 7 }, { admit() {} });
implementAction(revise, {
  // @ts-expect-error 业务返回 output 的原始 string，不能再次解析已转换的 Date。
  execute: () => new Date(),
});
// @ts-expect-error 业务实现不能替换纯合同所拥有的展示投影。
implementAction(revise, { execute: () => '2026-10-26T00:00:00.000Z', observe: () => ({ wrong: true }) });
