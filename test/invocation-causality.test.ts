import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { tap } from 'rxjs';
import { defineAction, implementAction, defineApplication, defineEnvironment, defineProgram, defineSurface,
  createWorkSelector, z, type ActionInvocation, type ConversationEvent } from '@hanger-source/dsh-reactive';
import { implementProgram, Offer, useAction, useRelation } from '@hanger-source/dsh-reactive/program';
import { boot, text, tool } from './native-support.ts';

for (const { mode, reuse, reopen } of [
  { mode: 'native', reuse: true, reopen: true },
  { mode: 'ptc', reuse: true, reopen: false },
  { mode: 'ptc', reuse: false, reopen: true },
] as const) test(`${mode}: ${reuse ? '复用' : '不同'}provider标签的Invocation与原生终态精确关联${reopen ? '，冷读不重执行' : '（实时）'}`, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-invocation-causality-'));
  const actionContract = defineAction({ name: 'record_fact', description: '读取本工作的业务事实', scope: z.null(),
    input: z.object({}).strict(), output: z.number().int() });
  const invocations: ActionInvocation<typeof actionContract>[] = [];
  const events: ConversationEvent[] = [], order: string[] = [];
  let executions = 0, models = 0, policies = 0;
  const implementation = implementAction(actionContract, { execute(_input, call) {
    order.push('execute:' + call.invocationId); return ++executions;
  } });
  const program = defineProgram({ input: z.object({ workId: z.string() }), surface: defineSurface({ views: {}, actions: {} }) });
  const definition = defineApplication({ name: 'invocation-causality', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '沿同一次明确工作读取业务事实', program: implementProgram(program, { Component() {
      const action = useAction(implementation, () => null, []);
      useRelation(() => action.invocations$.pipe(tap(value => invocations.push(value))), [action]);
      return Offer({ action });
    } }),
  })] });
  const responding = async function* () {
    if (++models <= 2) {
      const callId = reuse ? 'provider-reused' : 'provider-' + models;
      if (mode === 'native') yield* tool('record_fact', callId, {});
      else yield* tool('run_code', callId, { description: '再次读取同一工作', code: 'return await tools.record_fact({});' });
    } else yield* text('两次业务读取已结算，原生投递结果各自保留');
  };
  let app = await boot(root, definition, responding, mode);
  const stop = app.native.on('tools/post-execute', (exec, _result, next) => {
    if (exec.name === 'record_fact' && ++policies === 1) return Promise.resolve({ kind: 'block' as const,
      feedback: [{ type: 'text' as const, text: '本次业务已返回，后置投递政策拒绝交付' }] });
    return next();
  });
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const subscription = session.rawEvents$.subscribe(event => {
      events.push(event); if (event.kind === 'tool.invocation') order.push('associated:' + event.invocationId);
    });
    await session.submit({ requestId: 'original', content: '为同一工作读取两次现场', intent: 'question', delivery: 'followup', scope: { workId: 'inspection' } });
    await session.whenIdle();
    assert.equal(invocations.length, 2); assert.deepEqual(await Promise.all(invocations.map(value => value.result)), [1, 2]);
    assert.deepEqual(await Promise.all(invocations.map(async value => (await value.activity.read()).state)), ['returned', 'returned']);
    if (reuse) assert.deepEqual(invocations[0]!.source, invocations[1]!.source, 'provider标签和同行输入不足以区分实际发生');
    const calls = session.snapshot().calls.filter(call => call.name === 'record_fact');
    assert.equal(calls.length, 2); assert.deepEqual(calls[0]!.inputs, calls[1]!.inputs);
    for (const [index, invocation] of invocations.entries()) {
      const call = calls.find(value => value.invocationId === invocation.id);
      assert.ok(call); assert.equal(call.state, index === 0 ? 'failed' : 'returned');
      const associated = events.find(event => event.kind === 'tool.invocation' && event.invocationId === invocation.id);
      assert.ok(associated && associated.kind === 'tool.invocation'); assert.equal(associated.callSequence, call.callSequence);
      assert.ok(order.indexOf('associated:' + invocation.id) < order.indexOf('execute:' + invocation.id));
      const started = events.findIndex(event => event.kind === 'tool.started' && event.callSequence === call.callSequence);
      const linked = events.indexOf(associated);
      const settled = events.findIndex(event => event.kind === 'tool.settled' && event.callSequence === call.callSequence
        && event.invocationId === invocation.id);
      assert.ok(started >= 0 && started < linked && linked < settled);
    }
    if (mode === 'ptc') assert.ok(session.snapshot().calls.filter(call => call.name === 'run_code').every(call => call.invocationId === null));
    const selector = createWorkSelector({ identify: origin => origin.kind === 'invocation' ? undefined : z.object({ workId: z.string() }).parse(origin.scope).workId });
    const work = await selector.observe('inspection', { sessions: [session] }).state.read();
    assert.deepEqual(work.sessions[0]!.calls.filter(call => call.value.name === 'record_fact').map(call => call.value.invocationId), invocations.map(value => value.id));
    if (!reopen) {
      // rc.3 bridge 对重复 root 标签生成相同 child ID，正式日志读取器拒绝其持久恢复。
      // 此输入的实时关系在这里验证；合法不同 root 标签的 PTC 冷恢复由并列场景验证。
      assert.equal(models, 3); assert.equal(executions, 2); assert.deepEqual(app.errors, []);
      subscription.unsubscribe(); session.release(); return;
    }
    const saved = structuredClone(session.snapshot().calls), id = session.id;
    subscription.unsubscribe(); stop(); await app.host.close(id); session.release(); await app.dispose();
    app = await boot(root, definition, responding, mode);
    const cold = await app.host.open(id, program);
    const replayed: ConversationEvent[] = [];
    const coldEvents = cold.rawEvents$.subscribe(event => replayed.push(event));
    assert.deepEqual(cold.snapshot().calls, saved);
    assert.deepEqual((await selector.observe('inspection', { sessions: [cold] }).state.read()).sessions[0]!.calls
      .filter(call => call.value.name === 'record_fact').map(call => call.value.invocationId), invocations.map(value => value.id));
    assert.equal(models, 3); assert.equal(executions, 2); assert.deepEqual(replayed, []);
    assert.deepEqual(app.errors, []); coldEvents.unsubscribe(); cold.release();
  } finally { stop(); await app.dispose(); }
});
