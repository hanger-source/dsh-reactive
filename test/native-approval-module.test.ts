import assert from 'node:assert/strict';
import { test } from 'node:test';
import { access, mkdtemp, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createElement as h, Fragment } from 'react';
import { BehaviorSubject, filter, firstValueFrom, from, timeout } from 'rxjs';
import { approvalSurface, composeSurface, defineApplication, defineEnvironment, defineProgram, z,
  type ApprovalResponderAdmission } from '@hanger-source/dsh-reactive';
import { implementProgram, NativeTools, useObservable } from '@hanger-source/dsh-reactive/program';
import { createNativeApprovalModule, type NativeApprovalEvent } from '@hanger-source/dsh-reactive-dsh/approval';
import { type ConversationHandle } from '@hanger-source/dsh-reactive-dsh';
import { boot, text, tool } from './native-support.ts';

for (const boundary of ['cancel', 'unmount'] as const) test(`原生审批模块${boundary}终结真实等待，异步准入后的迟答不能执行工具`, { timeout: 15_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), `dsh-reactive-approval-module-${boundary}-`));
  const mounted = new BehaviorSubject(true);
  const entered = Promise.withResolvers<ApprovalResponderAdmission>(), release = Promise.withResolvers<void>();
  const settled = Promise.withResolvers<void>();
  const audit: NativeApprovalEvent[] = [];
  let planned = true, executions = 0;
  const approval = createNativeApprovalModule({ policy: 'ask',
    require: execution => execution.name === 'approval_effect' ? { reason: '仅人工批准可写入' } : undefined,
    async admitResponder(admission) {
      assert.equal(admission.actorId, 'operator-test'); entered.resolve(admission); await release.promise;
    },
  });
  const layout = composeSurface({ approval: approvalSurface });
  const program = defineProgram({ input: z.null(), surface: layout.surface });
  const definition = defineApplication({ name: 'approval-module-lifecycle', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '仅执行仍有效的人工批准调用', program: implementProgram(program, { modules: { approval },
      Component({ modules }) {
        const active = useObservable(mounted);
        return h(Fragment, null, active.status === 'ready' && active.value
          ? h(modules.approval.Component, { mount: layout.mounts.approval }) : null,
        h(NativeTools, { names: ['approval_effect'] }));
      },
    }),
  })] });
  const app = await boot(join(root, 'home'), definition, async function* () {
    if (planned) { planned = false; yield* tool('approval_effect', 'approval-' + boundary, {}); }
    else yield* text('已处理工具结算');
  });
  let session: ConversationHandle<typeof program> | undefined;
  const removeAudit = app.native.on('session/event', (current, event) => {
    if (current.id !== session?.id || event.type !== 'approval/asked' && event.type !== 'approval/decided') return;
    audit.push(event);
    if (event.type === 'approval/decided') settled.resolve();
  });
  const file = join(root, 'must-not-exist.txt');
  const removeTool = app.native.get('tools')!.register({ name: 'approval_effect', description: '批准后才可创建实际文件',
    parameters: { type: 'object', properties: {}, additionalProperties: false },
    output: { schema: { type: 'null' }, render: () => [{ type: 'text', text: 'null' }] },
    async execute() {
      executions++;
      // 独立文件副作用由生产工具执行；取消与撤挂必须让此处无法到达。
      await writeFile(file, 'TOOL_EXECUTED'); return null;
    },
  });
  const wait = <T>(promise: Promise<T>) => firstValueFrom(from(promise).pipe(timeout(5_000)));
  try {
    session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const module = session.module(layout.mounts.approval), pending = module.viewResource('pending');
    const waiting = firstValueFrom(pending.values$.pipe(filter(value => value.length === 1), timeout(5_000)));
    void waiting.catch(() => {});
    await session.submit({ requestId: 'pending', content: '等待人工批准后写入', intent: 'question', delivery: 'followup', scope: null });
    const [request] = await waiting;
    await assert.rejects(access(file), { code: 'ENOENT' });
    const invoke = module.resolveAction('answer', { sessionId: session.id });
    const invocation = invoke({ transportId: request!.transportId, decision: 'allowed-once' });
    const late = invocation.result;
    void late.catch(() => {});
    const admission = await wait(entered.promise);
    assert.equal(admission.identity.sessionId, session.id);
    assert.equal(admission.signal.aborted, false);
    if (boundary === 'cancel') await session.cancel(); else mounted.next(false);
    await wait(settled.promise);
    assert.equal(admission.signal.aborted, true);
    await session.whenIdle();
    await assert.rejects(access(file), { code: 'ENOENT' });
    release.resolve();
    await assert.rejects(late);
    assert.equal(executions, 0);
    await assert.rejects(access(file), { code: 'ENOENT' });
    const asked = audit.filter(event => event.type === 'approval/asked');
    const decided = audit.filter(event => event.type === 'approval/decided');
    assert.equal(asked.length, 1);
    assert.deepEqual(decided.map(event => event.data.id), asked.map(event => event.data.id));
    assert.deepEqual(decided.map(event => event.data.outcome), [boundary === 'cancel' ? 'cancelled' : 'unavailable']);
    assert.deepEqual(app.errors, []);
  } finally {
    release.resolve(); session?.release(); removeAudit(); removeTool(); mounted.complete(); await app.dispose();
  }
});
