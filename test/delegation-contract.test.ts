import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { LlmAdapter } from '@deepseek-ai/dsh-llm';
import { SessionId } from '@deepseek-ai/dsh-session';
import { defineAction, defineApplication, defineEnvironment, defineProgram, defineSurface, nativeChildWorkAction,
  NativeChildWork, programIdentity, SessionProgramMismatchError, z, type NativeChildCommand, type ProgramContract } from '@hanger-source/dsh-reactive';
import { Expose, implementProgram, useAction } from '@hanger-source/dsh-reactive/program';
import { ChildCreationIncompleteError, createRuntime, nativeModel } from '@hanger-source/dsh-reactive-dsh';

test('原生委托按原Action与子Program持久身份恢复，未出生的child也不能被换约接管', async t => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-delegation-contract-'));
  const errors: unknown[] = [], reads: number[] = [];
  let models = 0;
  const action = (version: number) => defineAction({ name: 'delegate_versioned', version, description: '独立合同的子程序委托',
    scope: z.null(), input: z.string(), ...nativeChildWorkAction });
  const original = action(1), revised = action(2);
  const child = defineProgram({ input: z.string(), surface: defineSurface({ views: {}, actions: {} }) });
  let childParses = 0;
  const sharedInput = z.string().transform(value => { childParses++; return value; });
  const shared = defineProgram({ name: 'delegation_contract_child', input: sharedInput, surface: child.surface });
  const changed = defineProgram({ ...shared, version: shared.version + 1 });
  const renamed = defineProgram({ ...shared, name: 'delegation_contract_other_child' });
  const runtimes: Awaited<ReturnType<typeof createRuntime>>[] = [];
  const parents: { release(): void }[] = [];
  t.after(async () => { parents.forEach(parent => parent.release()); for (const runtime of runtimes) await runtime.dispose(); });
  const boot = async (includeOriginal: boolean, sessionId?: string, selected: ProgramContract = child) => {
    const surface = defineSurface({ views: {}, actions: { v1: original, v2: revised } });
    const program = defineProgram({ input: z.null(), surface });
    const childImplementation = implementProgram(selected, { Component: () => null });
    const runtime = await createRuntime(assembly => {
      const implementation = (definition: typeof original) => assembly.implementDelegation(definition, {
        mode: 'spawn', program: childImplementation, childInput: input => input, objective: input => input,
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
  assert.deepEqual(reads, [1, 2, 2, 1]);
  restored.parent.release(); await restored.runtime.dispose();
  const promoted = await boot(false, parentId, shared);
  await assert.rejects(promoted.parent.locateOperation(NativeChildWork, commands[1]!), error => error instanceof SessionProgramMismatchError
    && error.recorded === null && error.current?.name === shared.name);
  assert.equal(childParses, 0, '无名旧意图不能由当前具名Program重解释或先解析childInput');
  promoted.parent.release(); await promoted.runtime.dispose();

  const originalShared = await boot(false, undefined, shared);
  const invocation = originalShared.parent.resolveAction('v2', null)('原共享子工作');
  await assert.rejects(invocation.result, error => error instanceof ChildCreationIncompleteError);
  const reserved = await originalShared.parent.locateOperation(NativeChildWork,
    { parentSessionId: originalShared.parent.id, invocationId: invocation.id });
  assert.equal(reserved.status, 'accepted');
  if (reserved.status !== 'accepted') throw new Error('PERSISTED_CHILD_RESERVATION_REQUIRED');
  const reference = reserved.reference;
  assert.equal(await originalShared.runtime.native.sessionPersistence.stat(SessionId(reference.childSessionId)), undefined,
    '真实深度限制发生在意图已持久、child尚未出生的边界');
  const sharedParentId = originalShared.parent.id;
  const sharedCommand = { parentSessionId: sharedParentId, invocationId: invocation.id };
  originalShared.parent.release(); await originalShared.runtime.dispose();
  for (const [alternative, expected] of [[changed, programIdentity(changed)], [renamed, programIdentity(renamed)], [child, null]] as const) {
    const replacement = await boot(false, sharedParentId, alternative);
    const parsesBefore: number = childParses, admissionsBefore = reads.length;
    await assert.rejects(replacement.parent.locateOperation(NativeChildWork, sharedCommand), error => {
      assert.ok(error instanceof SessionProgramMismatchError);
      assert.equal(error.sessionId, reference.childSessionId); assert.deepEqual(error.recorded, programIdentity(shared));
      assert.deepEqual(error.current, expected);
      return true;
    });
    assert.equal(childParses, parsesBefore, '持久Program不符须在旧childInput解析之前拒绝');
    assert.equal(reads.length, admissionsBefore, '新Program的authorize不能接管旧意图');
    assert.equal(await replacement.runtime.native.sessionPersistence.stat(SessionId(reference.childSessionId)), undefined);
    replacement.parent.release(); await replacement.runtime.dispose();
  }
  const recovered = await boot(false, sharedParentId, shared);
  assert.equal((await recovered.parent.locateOperation(NativeChildWork, sharedCommand)).status, 'accepted');
  recovered.parent.release(); await recovered.runtime.dispose();
  assert.equal(models, 0); assert.deepEqual(errors, []);
});
