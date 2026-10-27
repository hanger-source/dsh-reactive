import { implementProgram } from '@hanger-source/dsh-reactive/program';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createElement } from 'react';
import { tap } from 'rxjs';
import { defineApplication, defineEnvironment, z, type ConversationEvent } from '@hanger-source/dsh-reactive';
import { defineAction, implementAction } from '@hanger-source/dsh-reactive';
import { defineProgram } from '@hanger-source/dsh-reactive';
import { defineSurface } from '@hanger-source/dsh-reactive';
import { Offer } from '@hanger-source/dsh-reactive/program';
import { useAction, useRelation } from '@hanger-source/dsh-reactive/program';
import type { GenerateOptions } from '@deepseek-ai/dsh-llm';
import { validateJsonSchemaValue } from '@deepseek-ai/dsh-tools';
import { toolSchema } from '../packages/dsh/src/execution/schema.ts';
import { boot, text, tool } from './native-support.ts';

test('原生工具的完整 Zod 输入输出合同只解析一次，转换结果一致进入业务、公开反馈和下一模型步', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-contracts-'));
  let inputParses = 0;
  let outputParses = 0;
  const executed: number[] = [];
  const observed: number[] = [];
  const events: ConversationEvent<null>[] = [];
  const requests: GenerateOptions[] = [];
  const calculateTotal = defineAction({ name: 'calculate_total', description: '将金额翻倍后生成正式合计', scope: z.null(),
    input: z.object({ amount: z.string().transform(value => { inputParses++; return Number(value); }).pipe(z.number()) }).strict(),
    output: z.object({ total: z.string().transform(value => { outputParses++; return Number(value) + 1; }).pipe(z.number()) }).strict(),
    observe(output) {
      const total: number = output.total;
      observed.push(total);
      return { total };
    },
  });
  const calculateTotalImplementation = implementAction(calculateTotal, {
    async execute(input) {
      const amount: number = input.amount;
      executed.push(amount);
      return { total: String(amount * 2) };
    }
  });
  const definition = defineApplication({ name: 'contracts', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '调用正式合计能力，核对返回的数字结果',
    program: implementProgram(defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) }), { Component({ conversation }) {
        const calculate = useAction(calculateTotalImplementation, () => null, []);
        useRelation(() => conversation.events$.pipe(tap(event => events.push(event))), [conversation]);
        return createElement(Offer<typeof calculateTotal>, { name: 'calculate_total', action: calculate });
      } }),
  })] });
  const app = await boot(join(root, 'home'), definition, async function* (options) {
    requests.push(options);
    if (requests.length === 1) yield* tool('calculate_total', 'contract-call', { amount: '21' });
    else yield* text('已接收合计 43');
  });
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    await session.submit({ requestId: 'calculate-request', content: '计算金额 21 的正式合计', intent: 'question', delivery: 'followup' as const, scope: null });
    await session.whenIdle();

    assert.deepEqual(app.errors, []);
    assert.equal(requests.length, 2);
    assert.equal(inputParses, 1);
    assert.equal(outputParses, 1);
    assert.deepEqual(executed, [21]);
    assert.ok(observed.length > 0);
    assert.ok(observed.every(total => total === 43));
    assert.deepEqual(requests[0]!.tools?.find(value => value.name === 'calculate_total')?.parameters,
      { type: 'object', properties: { amount: { type: 'string' } }, required: ['amount'], additionalProperties: false });

    const settled = events.filter((event): event is Extract<ConversationEvent<null>, { kind: 'tool.settled' }> =>
      event.kind === 'tool.settled' && event.name === 'calculate_total');
    assert.equal(settled.length, 1);
    assert.equal(settled[0]!.callId, 'contract-call');
    assert.equal(settled[0]!.inputs[0]!.requestId, 'calculate-request');
    assert.equal(settled[0]!.outcome, 'returned');
    assert.deepEqual(settled[0]!.result, { total: 43 });

    const results = requests[1]!.messages.filter(message => message.role === 'tool' && message.toolCallId === 'contract-call');
    assert.equal(results.length, 1);
    assert.equal(results[0]!.role, 'tool');
    const result = results[0]!;
    assert.ok(result.role === 'tool');
    assert.ok(!result.isError);
    const resultText = result.content.flatMap(block => block.type === 'text' ? [block.text] : []);
    assert.equal(resultText.length, 1);
    assert.deepEqual(JSON.parse(resultText[0]!), settled[0]!.result);
    assert.equal(session.snapshot().requests['calculate-request']?.reply, '已接收合计 43');
    assert.equal(session.snapshot().requests['calculate-request']?.tools[0]?.state, 'returned');
    assert.deepEqual(session.snapshot().requests['calculate-request']?.tools[0]?.result, settled[0]!.result);
    session.release();
  } finally { await app.dispose(); }
});

test('嵌套JSON和开放JSON记录经原生工具往返，数字业务约束仍由原Zod合同在owner前拒绝', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-json-contracts-'));
  const input = z.object({ expectedRevision: z.number().int().positive(),
    definition: z.record(z.string(), z.json().describe('配置条目')).optional(),
    agentConfiguration: z.record(z.string(), z.json()),
    payload: z.json().describe('原始嵌套输入正文').nullable(),
  }).strict();
  const output = z.json().nullable().describe('已登记的完整配置');
  const valid: z.input<typeof input> = { expectedRevision: 7,
    definition: { routes: [{ merchantId: 'merchant-a', enabled: true }, null, ['nested', 3]], description: '正式配置' },
    agentConfiguration: { retry: { attempts: 3, delay: null }, active: true, labels: ['资料', '交付'] },
    payload: { revisions: [1, 7], previous: null, metadata: { suppliedBy: 'operator' } },
  };
  const rejected = { ...valid, expectedRevision: 0 };
  const expected = { revision: 7, definition: valid.definition!, agentConfiguration: valid.agentConfiguration, payload: valid.payload };
  const executed: z.output<typeof input>[] = [];
  const events: ConversationEvent<null>[] = [];
  const requests: GenerateOptions[] = [];
  const publish = defineAction({ name: 'publish_configuration', description: '按正式修订登记完整JSON配置', scope: z.null(), input, output });
  const implementation = implementAction(publish, { execute(value) {
    executed.push(structuredClone(value));
    return { revision: value.expectedRevision, agentConfiguration: value.agentConfiguration, payload: value.payload,
      ...(value.definition === undefined ? {} : { definition: value.definition }) };
  } });
  const definition = defineApplication({ name: 'json-contracts', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '登记完整配置并核对正式输入拒绝，不把工具声明注解当成业务验证',
    program: implementProgram(defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) }), { Component({ conversation }) {
      const action = useAction(implementation, () => null, []);
      useRelation(() => conversation.events$.pipe(tap(event => events.push(event))), [conversation]);
      return createElement(Offer<typeof publish>, { name: 'publish_configuration', action });
    } }),
  })] });
  // 原生支持子集不执行positive约束；接下来的真实调用必须由原Zod合同拒绝0。
  assert.deepEqual(validateJsonSchemaValue(toolSchema(input), rejected), []);
  assert.throws(() => toolSchema(z.record(z.string(), z.number())), /REACTIVE_TOOL_SCHEMA_UNSUPPORTED/u);
  assert.throws(() => toolSchema(z.record(z.string().min(1), z.json())), /REACTIVE_TOOL_SCHEMA_UNSUPPORTED/u);
  type RecursiveNode = { children: RecursiveNode[] };
  const recursive: z.ZodType<RecursiveNode> = z.lazy(() => z.object({ children: recursive.array() }));
  assert.throws(() => toolSchema(recursive), /REACTIVE_TOOL_SCHEMA_UNSUPPORTED/u);
  assert.throws(() => toolSchema(z.string().meta({ $ref: '#/definitions/missing' })), /REACTIVE_TOOL_SCHEMA_UNSUPPORTED/u);
  const app = await boot(join(root, 'home'), definition, async function* (options) {
    requests.push(options);
    if (requests.length === 1) yield* tool('publish_configuration', 'valid-json-call', valid);
    else if (requests.length === 2) yield* tool('publish_configuration', 'rejected-json-call', rejected);
    else yield* text('配置已登记，非法修订已拒绝');
  });
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    await session.submit({ requestId: 'publish-json', content: '登记嵌套配置，再核对非法修订不会执行',
      intent: 'question', delivery: 'followup', scope: null });
    await session.whenIdle();
    assert.deepEqual(app.errors, []);
    assert.equal(requests.length, 3);
    assert.deepEqual(executed, [valid]);
    const settled = events.filter((event): event is Extract<ConversationEvent<null>, { kind: 'tool.settled' }> =>
      event.kind === 'tool.settled' && event.name === 'publish_configuration');
    assert.equal(settled.length, 2);
    assert.equal(settled[0]!.outcome, 'returned');
    assert.equal(settled[0]!.callId, 'valid-json-call');
    assert.equal(settled[1]!.outcome, 'failed');
    assert.equal(settled[1]!.callId, 'rejected-json-call');
    const accepted = requests[1]!.messages.find(message => message.role === 'tool' && message.toolCallId === 'valid-json-call');
    assert.ok(accepted?.role === 'tool');
    assert.ok(!accepted.isError);
    assert.deepEqual(accepted.content.flatMap(part => part.type === 'text' ? [JSON.parse(part.text)] : []), [expected]);
    const refusal = requests[2]!.messages.find(message => message.role === 'tool' && message.toolCallId === 'rejected-json-call');
    assert.ok(refusal?.role === 'tool' && refusal.isError);
    assert.equal(session.snapshot().requests['publish-json']?.reply, '配置已登记，非法修订已拒绝');
    session.release();
  } finally { await app.dispose(); }
});
