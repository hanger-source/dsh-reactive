import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { LlmAdapter } from '@deepseek-ai/dsh-llm';
import { defineAction, defineApplication, defineEnvironment, defineProgram, defineSurface, nativeChildWorkAction,
  NativeChildWork, z, type NativeChildCommand } from '@hanger-source/dsh-reactive';
import { Expose, implementProgram, useAction } from '@hanger-source/dsh-reactive/program';
import { ChildCreationIncompleteError, createRuntime, nativeModel } from '@hanger-source/dsh-reactive-dsh';

test('原生委托持久恢复按原Action版本准入，同名新版不能接管旧命令且不启动模型', async t => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-delegation-contract-'));
  const errors: unknown[] = [], reads: number[] = [];
  let models = 0;
  const action = (version: number) => defineAction({ name: 'delegate_versioned', version, description: '独立合同的子程序委托',
    scope: z.null(), input: z.string(), ...nativeChildWorkAction });
  const original = action(1), revised = action(2);
  const child = implementProgram(defineProgram({ input: z.string(), surface: defineSurface({ views: {}, actions: {} }) }), { Component: () => null });
  const runtimes: Awaited<ReturnType<typeof createRuntime>>[] = [];
  const parents: { release(): void }[] = [];
  t.after(async () => { parents.forEach(parent => parent.release()); for (const runtime of runtimes) await runtime.dispose(); });
  const boot = async (includeOriginal: boolean, sessionId?: string) => {
    const surface = defineSurface({ views: {}, actions: { v1: original, v2: revised } });
    const program = defineProgram({ input: z.null(), surface });
    const runtime = await createRuntime(assembly => {
      const implementation = (definition: typeof original) => assembly.implementDelegation(definition, {
        mode: 'spawn', program: child, childInput: input => input, objective: input => input,
        authorize(admission) { if (admission.phase === 'read') reads.push(definition.version); },
      });
      const one = includeOriginal ? implementation(original) : undefined, two = implementation(revised);
      return defineApplication({ name: 'delegation-contract', admit: async () => {}, environments: [defineEnvironment({
        id: 'work', instructions: '只作显式SDK调用与冷恢复', program: implementProgram(program, { Component() {
          const v2 = useAction(two, () => null, []);
          // 本次Runtime装配固定，不在运行中的组件里切换hook数量。
          const v1 = one ? useAction(one, () => null, []) : undefined;
          const actions = { v2, ...(v1 ? { v1 } : {}) };
          return Expose({ surface, value: { views: {}, actions } });
        } }),
      })] });
    }, { home: join(root, 'home'), actorId: 'delegation-user', subagents: { maxDepth: 0 },
      onError: event => errors.push(event.error), model: nativeModel({ provider: 'delegation-contract', model: 'unused',
        create: () => new class extends LlmAdapter { async *stream(): AsyncGenerator<never> { models++; throw new Error('COLD_RECOVERY_MUST_NOT_START_MODEL'); } }(),
      }),
    }); runtimes.push(runtime);
    const workspace = await runtime.application.createWorkspace({ directory: root });
    const parent = sessionId ? await runtime.application.resume(sessionId, program)
      : await runtime.application.create('work', { workspaceId: workspace.workspaceId });
    parents.push(parent);
    return { runtime, parent };
  };
  const first = await boot(true);
  const commands: NativeChildCommand[] = [];
  // 原生公开深度限制在真实持久保留之后拒绝子创建；没有替换运行引擎或模拟日志。
  for (const slot of ['v1', 'v2'] as const) {
    const invocation = first.parent.resolveAction(slot, null)('独立子工作' + slot);
    await assert.rejects(invocation.result, error => error instanceof ChildCreationIncompleteError);
    const command = { parentSessionId: first.parent.id, invocationId: invocation.id };
    const location = await first.parent.locateOperation(NativeChildWork, command);
    assert.equal(location.status, 'accepted');
    if (location.status === 'accepted') assert.equal(location.state.phase, 'creation-incomplete');
    commands.push(command);
  }
  assert.deepEqual(reads, [1, 2]);
  const parentId = first.parent.id; first.parent.release(); await first.runtime.dispose();
  const newer = await boot(false, parentId);
  await assert.rejects(newer.parent.locateOperation(NativeChildWork, commands[0]!), /REACTIVE_DELEGATION_IMPLEMENTATION_REQUIRED/);
  assert.deepEqual(reads, [1, 2], '同名新版的authorize不得接管旧命令');
  assert.equal((await newer.parent.locateOperation(NativeChildWork, commands[1]!)).status, 'accepted');
  newer.parent.release(); await newer.runtime.dispose();
  const restored = await boot(true, parentId);
  assert.equal((await restored.parent.locateOperation(NativeChildWork, commands[0]!)).status, 'accepted');
  assert.deepEqual(reads, [1, 2, 2, 1]); assert.equal(models, 0); assert.deepEqual(errors, []);
});
