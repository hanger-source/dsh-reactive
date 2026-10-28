import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, mkdir, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createElement as h, Fragment } from 'react';
import { scopeOf } from '@deepseek-ai/dsh-scope';
import { SessionId } from '@deepseek-ai/dsh-session';
import { BehaviorSubject, Observable, filter, firstValueFrom, of, timeout } from 'rxjs';
import { defineAction, defineApplication, defineEnvironment, defineProgram, defineSurface, implementAction, view, z } from '@hanger-source/dsh-reactive';
import { Context, Expose, Offer, Skill, implementProgram, useAction, useObservable } from '@hanger-source/dsh-reactive/program';
import { boot } from './native-support.ts';

test('Program失败撤销同一Agent的原生声明与目录Skill，已受理业务和其他会话继续独立完成', { timeout: 20_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-program-native-owner-'));
  const workspaces = [join(directory, 'one'), join(directory, 'two')];
  for (const [index, workspace] of workspaces.entries()) {
    const method = join(workspace, 'method');
    await mkdir(method, { recursive: true });
    await writeFile(join(method, 'SKILL.md'), '---\nname: native-ownership-method\ndescription: 当前会话实际目录方法\n---\n\nNATIVE_METHOD_' + index + '\n');
  }
  const failedSession = new BehaviorSubject<string | null>(null);
  const failure = new Error('PROGRAM_COMPONENT_FAILED_AFTER_ADMISSION');
  const entered = Promise.withResolvers<AbortSignal>(), proceed = Promise.withResolvers<void>();
  let executions = 0, models = 0;
  const work = defineAction({ name: 'native_ownership_work', description: '已受理业务由原调用继续结算',
    scope: z.object({ sessionId: z.string() }), input: z.object({ value: z.string() }), output: z.object({ value: z.string() }) });
  const implementation = implementAction(work, { async execute(input, call) {
    executions++;
    entered.resolve(call.signal);
    await proceed.promise;
    call.signal.throwIfAborted();
    return { value: input.value };
  } });
  const surface = defineSurface({ views: { state: view(z.string()) }, actions: { work } });
  const program = defineProgram({ input: z.null(), surface });
  const definition = defineApplication({ name: 'program-native-owner', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '原生声明安装与业务执行保留各自归属。',
    program: implementProgram(program, { Component({ conversation, workspace }) {
      const selected = useObservable(() => failedSession, []);
      const action = useAction(implementation, () => ({ sessionId: conversation.identity.sessionId }), [conversation]);
      if (selected.status === 'ready' && selected.value === conversation.identity.sessionId) throw failure;
      return h(Fragment, null,
        Context({ name: 'native_ownership_context', value: { sessionId: conversation.identity.sessionId } }),
        Offer({ action }),
        Skill({ directory: join(workspace.directory, 'method') }),
        Expose({ surface, value: { views: { state: of('available') }, actions: { work: action } } }));
    } }),
  })] });
  const app = await boot(join(directory, 'home'), definition, async function* () {
    models++; throw new Error('NATIVE_DECLARATION_LIFETIME_MUST_NOT_CALL_MODEL');
  }, 'native', workspaces[0]!);
  const first = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
  const secondWorkspace = await app.host.createWorkspace({ directory: workspaces[1]! });
  const second = await app.host.create('work', { workspaceId: secondWorkspace.workspaceId });
  try {
    const one = app.native.agents.get(SessionId(first.id))!, two = app.native.agents.get(SessionId(second.id))!;
    assert.ok(one); assert.ok(two);
    const signal = new AbortController().signal;
    const catalog = (agent: typeof one, cwd: string) => app.native.skills.list({ scope: agent, cwd, signal });
    assert.ok((await catalog(one, workspaces[0]!)).some(skill => skill.name === 'native-ownership-method'));
    assert.ok((await catalog(two, workspaces[1]!)).some(skill => skill.name === 'native-ownership-method'));
    assert.ok(app.native.tools.schemas(one).some(tool => tool.name === work.name));
    assert.ok(app.native.tools.schemas(two).some(tool => tool.name === work.name));
    const before = await app.native.systemPrompt.assemble({ scope: one, signal });
    assert.ok(before.contexts.some(context => context.name === 'native_ownership_context'));
    const action = first.resolveAction('work', { sessionId: first.id });
    const invocation = action({ value: 'accepted-before-failure' });
    const settlement = Promise.allSettled([invocation.result]);
    const executionSignal = await entered.promise;
    const observedFailure = firstValueFrom(first.viewResource('state').observations$.pipe(
      filter(observation => observation.status === 'error'), timeout(5_000)));
    const nativeExit = firstValueFrom(new Observable<void>(subscriber => app.native.on('internal/status', fiber => {
      // 仅观察公开Fiber状态，不能以主动dispose推动被测安装退出。
      if (fiber.name === 'reactive-program-bindings' && scopeOf(fiber.ctx) === one && fiber.state === 4) subscriber.next();
    })).pipe(timeout(5_000)));
    const failedAndClosed = Promise.all([observedFailure, nativeExit]);
    failedSession.next(first.id);
    const [observation] = await failedAndClosed;
    assert.equal(observation.status, 'error');
    if (observation.status === 'error') assert.equal(observation.error, failure);
    await assert.rejects(first.readView('state'), error => error === failure);
    assert.equal(app.native.agents.get(SessionId(first.id)), one, '声明失败没有关闭实际Agent');
    assert.equal(executionSignal.aborted, false, 'Program退出没有取得已受理业务的取消权');
    assert.equal(app.native.tools.schemas(one).some(tool => tool.name === work.name), false);
    const after = await app.native.systemPrompt.assemble({ scope: one, signal });
    assert.equal(after.contexts.some(context => context.name === 'native_ownership_context'), false);
    assert.equal((await catalog(one, workspaces[0]!)).some(skill => skill.name === 'native-ownership-method'), false);
    assert.ok((await catalog(two, workspaces[1]!)).some(skill => skill.name === 'native-ownership-method'));
    assert.ok(app.native.tools.schemas(two).some(tool => tool.name === work.name));
    assert.ok((await app.native.systemPrompt.assemble({ scope: two, signal })).contexts.some(context => context.name === 'native_ownership_context'));
    assert.equal(await second.readView('state'), 'available');
    proceed.resolve();
    assert.deepEqual(await settlement, [{ status: 'fulfilled', value: { value: 'accepted-before-failure' } }]);
    assert.equal(executionSignal.aborted, false);
    assert.deepEqual(await second.resolveAction('work', { sessionId: second.id })({ value: 'other-session' }).result,
      { value: 'other-session' });
    await app.host.close(first.id);
    assert.equal(app.native.agents.get(SessionId(first.id)), undefined);
    const cold = await app.host.open(first.id, program);
    try {
      assert.deepEqual(await cold.requestStatus('never-submitted'), { requestId: 'never-submitted', state: 'absent' });
      await assert.rejects(cold.readView('state'), error => error === failure);
      assert.equal(app.native.agents.get(SessionId(first.id)), undefined);
    } finally { cold.release(); }
    assert.equal(executions, 2); assert.equal(models, 0); assert.deepEqual(app.errors, []);
  } finally { proceed.resolve(); failedSession.complete(); first.release(); second.release(); await app.dispose(); }
});

test('首次React求值失败保留Program原错，待安装声明退出且其他环境仍可创建', { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-program-native-initial-'));
  const failure = new Error('INITIAL_PROGRAM_RENDER_FAILED');
  const program = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) });
  const definition = defineApplication({ name: 'program-native-initial', admit: async () => {}, environments: [
    defineEnvironment({ id: 'failed', instructions: '首次求值抛出明确失败。',
      program: implementProgram(program, { Component() { throw failure; } }) }),
    defineEnvironment({ id: 'healthy', instructions: '独立环境仍可使用。',
      program: implementProgram(program, { Component: () => null }) }),
  ] });
  let models = 0;
  const app = await boot(join(directory, 'home'), definition, async function* () {
    models++; throw new Error('INITIAL_PROGRAM_FAILURE_MUST_NOT_CALL_MODEL');
  });
  try {
    const id = 'failed-program-at-initial-mount';
    await assert.rejects(app.host.create('failed', { workspaceId: app.workspace.workspaceId, sessionId: id }), error => error === failure);
    assert.equal(app.native.agents.get(SessionId(id)), undefined);
    assert.equal(app.native.sessions.get(SessionId(id)), undefined);
    const healthy = await app.host.create('healthy', { workspaceId: app.workspace.workspaceId });
    healthy.release();
    assert.equal(models, 0); assert.deepEqual(app.errors, []);
  } finally { await app.dispose(); }
});
