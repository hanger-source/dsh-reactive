import { implementProgram } from '@hanger-source/dsh-reactive/program';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createElement } from 'react';
import { tap } from 'rxjs';
import { LlmAdapter, type GenerateOptions, type ToolResultMessage } from '@deepseek-ai/dsh-llm';
import { defineApplication, defineEnvironment, z, type ConversationEvent } from '@hanger-source/dsh-reactive';
import { defineAction, implementAction } from '@hanger-source/dsh-reactive';
import { defineProgram } from '@hanger-source/dsh-reactive';
import { defineSurface } from '@hanger-source/dsh-reactive';
import { Offer } from '@hanger-source/dsh-reactive/program';
import { useAction, useRelation } from '@hanger-source/dsh-reactive/program';
import { createRuntime, nativeModel } from '@hanger-source/dsh-reactive-dsh';
import { text, tool } from './native-support.ts';

test('真实 QuickJS 拒绝宿主访问，CPU 预算终止无限循环后原生工具和宿主仍可继续工作', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-inline-'));
  // 与正式运行时默认 CPU 预算一致，墙钟 deadline 保持正式默认值。
  const cpuBudgetMs = 1_000;
  const requests: GenerateOptions[] = [];
  const errors: unknown[] = [];
  const events: ConversationEvent<null>[] = [];
  const ownerReads: number[] = [];
  let cpuStarted = 0;
  let cpuElapsed = 0;
  const readOwner = defineAction({ name: 'read_owner', description: '读取业务 owner 的正式修订', scope: z.null(),
    input: z.object({ revision: z.number() }).strict(), output: z.object({ revision: z.number(), owner: z.literal('business') }),
    observe: output => output });
  const readOwnerImplementation = implementAction(readOwner, {
    async execute(input) { ownerReads.push(input.revision); return { revision: input.revision, owner: 'business' as const }; }
  });
  const definition = defineApplication({ name: 'inline-boundary', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '通过程序调用受授权业务能力，失败后继续核对',
    program: implementProgram(defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) }), { Component({ conversation }) {
        const read = useAction(readOwnerImplementation, () => null, []);
        useRelation(() => conversation.events$.pipe(tap(event => events.push(event))), [conversation]);
        return createElement(Offer<typeof readOwner>, { name: 'read_owner', action: read });
      } }),
  })] });
  const runtime = await createRuntime(() => definition, { home: join(root, 'home'), actorId: 'operator-test', tools: 'ptc',
    execution: { cpuBudgetMs }, onError: event => errors.push(event.error),
    model: nativeModel({ provider: 'isolated', model: 'test', create: () => new class extends LlmAdapter {
      async *stream(options: GenerateOptions) {
        requests.push(options);
        if (requests.length === 1) {
          yield* tool('run_code', 'isolation-call', { description: '核对隔离边界和正式业务读取', code: `
            async function denied(action) {
              try { await action(); return false; } catch { return true; }
            }
            globalThis.previousGuest = true;
            return {
              process: await denied(() => process.cwd()),
              require: await denied(() => require('node:fs')),
              filesystemImport: await denied(() => import('node:fs')),
              networkImport: await denied(() => import('node:net')),
              bindingConstructor: await denied(() => tools.read_owner.constructor('return process')().cwd()),
              receipt: await tools.read_owner({ revision: 1 })
            };
          ` });
        } else if (requests.length === 2) {
          cpuStarted = performance.now();
          yield* tool('run_code', 'cpu-call', { description: '执行无限循环触发正式 CPU 预算',
            code: 'console.log("cpu-loop-entered"); while (true) {}' });
        } else if (requests.length === 3) {
          cpuElapsed = performance.now() - cpuStarted;
          yield* tool('run_code', 'recovery-call', { description: '预算失败后继续读取正式业务事实',
            code: 'return { fresh: typeof globalThis.previousGuest === "undefined", receipt: await tools.read_owner({ revision: 2 }) };' });
        } else yield* text('CPU 预算失败已结算，正式业务仍可读取');
      }
    }() }) });
  try {
    await mkdir(join(root, 'workspace'));
    const workspace = await runtime.application.createWorkspace({ directory: join(root, 'workspace') });
    const session = await runtime.application.create('work', { workspaceId: workspace.workspaceId });
    await session.submit({ requestId: 'boundary-request', content: '核对程序隔离与预算失败后的连续工作', intent: 'question', delivery: 'followup' as const, scope: null });
    await session.whenIdle();

    assert.deepEqual(errors, []);
    assert.equal(requests.length, 4);
    assert.ok(requests.every(request => request.tools?.some(value => value.name === 'run_code')));
    const isolation = resultOf(requests[1]!, 'isolation-call');
    assert.ok(!isolation.isError);
    assert.deepEqual(JSON.parse(resultText(isolation)), {
      process: true, require: true, filesystemImport: true, networkImport: true, bindingConstructor: true,
      receipt: { revision: 1, owner: 'business' },
    });

    const cpu = resultOf(requests[2]!, 'cpu-call');
    assert.equal(cpu.isError, true);
    assert.match(resultText(cpu), /code run failed \(timeout\): Inline CPU budget exceeded/u);
    assert.match(resultText(cpu), /Captured output:\ncpu-loop-entered/u);
    assert.ok(cpuElapsed >= cpuBudgetMs, '无限循环必须实际耗尽所声明的 CPU 预算');
    assert.ok(cpuElapsed < cpuBudgetMs * 10, 'CPU 中断应在正式预算附近返回，不能等待正式墙钟 deadline');

    const recovery = resultOf(requests[3]!, 'recovery-call');
    assert.ok(!recovery.isError);
    assert.deepEqual(JSON.parse(resultText(recovery)), { fresh: true, receipt: { revision: 2, owner: 'business' } });
    assert.deepEqual(ownerReads, [1, 2]);
    const settled = events.filter((event): event is Extract<ConversationEvent<null>, { kind: 'tool.settled' }> =>
      event.kind === 'tool.settled' && event.name === 'run_code');
    assert.deepEqual(settled.map(event => ({ callId: event.callId, outcome: event.outcome })), [
      { callId: 'isolation-call', outcome: 'returned' }, { callId: 'cpu-call', outcome: 'failed' },
      { callId: 'recovery-call', outcome: 'returned' },
    ]);
    assert.ok(settled.every(event => event.inputs[0]?.requestId === 'boundary-request'));
    assert.equal(session.snapshot().requests['boundary-request']?.reply, 'CPU 预算失败已结算，正式业务仍可读取');
    session.release();
  } finally { await runtime.dispose(); }
});

function resultOf(request: GenerateOptions, callId: string): ToolResultMessage {
  const results = request.messages.filter((message): message is ToolResultMessage => message.role === 'tool' && message.toolCallId === callId);
  assert.equal(results.length, 1);
  return results[0]!;
}
function resultText(result: ToolResultMessage): string {
  return result.content.flatMap(block => block.type === 'text' ? [block.text] : []).join('\n');
}
