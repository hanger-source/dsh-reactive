import assert from 'node:assert/strict';
import { test } from 'node:test';
import { access, mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createElement as h, Fragment } from 'react';
import { filter, firstValueFrom, from, timeout } from 'rxjs';
import WebSocket from 'ws';
import { LlmAdapter, ToolCallId } from '@deepseek-ai/dsh-llm';
import { approvalSurface, composeSurface, defineApplication, defineEnvironment, defineProgram, z } from '@hanger-source/dsh-reactive';
import { implementProgram, NativeTools } from '@hanger-source/dsh-reactive/program';
import { createRuntime, nativeModel } from '@hanger-source/dsh-reactive-dsh';
import { createNativeApprovalModule } from '@hanger-source/dsh-reactive-dsh/approval';
import { createProgramClient } from '@hanger-source/dsh-reactive-client';
import { mountProgramGateway, RemoteAuthenticationError } from '@hanger-source/dsh-reactive-remote';

const next = (source, predicate) => firstValueFrom(source.pipe(filter(predicate), timeout(5_000)));

test('远程人工经模块提交原生审批：观察退出、权限隔离与竞争保持真实工具仅执行一次', { timeout: 25_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-approval-participation-'));
  const layout = composeSurface({ approval: approvalSurface });
  const program = defineProgram({ name: 'approval_participation', input: z.null(), surface: layout.surface });
  const admitted = [], errors = [], audit = [], executions = [], clients = [], handles = [], offered = [];
  const bothEntered = Promise.withResolvers(), release = Promise.withResolvers();
  let concurrent = 0, planned, local, peer, gateway;
  const approval = createNativeApprovalModule({ policy: 'ask',
    require: execution => execution.name === 'approval_effect' ? { reason: '写入前需要人工批准' } : undefined,
    async admitResponder(admission) {
      assert.equal(admission.identity.sessionId, local.id);
      assert.equal(admission.request.toolName, 'approval_effect');
      if (!['bob', 'carol'].includes(admission.actorId)) throw new Error('APPROVAL_ACTOR_FORBIDDEN');
      admitted.push(admission.actorId);
      if (admission.request.callId === 'approval-first') {
        if (++concurrent === 2) bothEntered.resolve();
        await release.promise;
      }
    },
  });
  const definition = defineApplication({ name: 'approval-participation', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '只在人工批准后执行文件写入', program: implementProgram(program, { modules: { approval },
      Component: ({ modules }) => h(Fragment, null,
        h(modules.approval.Component, { mount: layout.mounts.approval }), h(NativeTools, { names: ['approval_effect'] })),
    }),
  })] });
  const runtime = await createRuntime(() => definition, { home: join(root, 'home'), actorId: 'owner', tools: 'native',
    onError: event => errors.push(event.error),
    model: nativeModel({ provider: 'approval-participation', model: 'controlled', create: () => new class extends LlmAdapter {
      async *stream(options) {
        offered.push(...(options.tools ?? []).map(tool => tool.name));
        const current = planned; planned = undefined;
        if (current) {
          const id = ToolCallId('approval-' + current), args = JSON.stringify({ marker: current });
          yield { type: 'block-start', index: 0, blockType: 'tool-call' };
          yield { type: 'tool-call-delta', index: 0, id, name: 'approval_effect', argumentsDelta: args };
          yield { type: 'block-end', index: 0, block: { type: 'tool-call', id, name: 'approval_effect', arguments: args } };
          yield { type: 'finish', reason: { kind: 'tool-calls' } };
        } else {
          yield { type: 'block-start', index: 0, blockType: 'text' };
          yield { type: 'block-end', index: 0, block: { type: 'text', text: '已处理工具结果' } };
          yield { type: 'finish', reason: { kind: 'stop' } };
        }
      }
    }() }),
  });
  const removeAudit = runtime.native.on('session/event', (session, event) => {
    if (session.id === local?.id && ['approval/asked', 'approval/decided'].includes(event.type)) audit.push(event);
  });
  const removeTool = runtime.native.get('tools').register({ name: 'approval_effect', description: '批准后写入实际调用身份',
    parameters: { type: 'object', properties: { marker: { type: 'string' } }, required: ['marker'], additionalProperties: false },
    output: { schema: { type: 'null' }, render: () => [{ type: 'text', text: 'null' }] },
    async execute(raw, execution) {
      const { marker } = z.object({ marker: z.enum(['first', 'second']) }).parse(raw);
      executions.push(execution.callId); await writeFile(join(root, marker + '.txt'), execution.callId); return null;
    },
  });
  const connect = async actorId => {
    const headers = { authorization: 'Bearer ' + actorId };
    const client = await createProgramClient(program, { baseUrl: gateway.url,
      fetch: (url, init) => fetch(url, { ...init, headers: { ...Object.fromEntries(new Headers(init?.headers)), ...headers } }),
      createWebSocket: url => new WebSocket(url, { headers }),
    });
    clients.push(client); return { client, session: await client.open(local.id) };
  };
  const answer = async (module, sessionId, transportId) => {
    const invoke = await module.resolveAction('answer', { sessionId });
    const invocation = await invoke({ transportId, decision: 'allowed-once' });
    return invocation.result;
  };
  try {
    const workspace = await runtime.application.createWorkspace({ directory: root });
    local = await runtime.application.create('work', { workspaceId: workspace.workspaceId }); handles.push(local);
    peer = await runtime.application.create('work', { workspaceId: workspace.workspaceId }); handles.push(peer);
    gateway = await mountProgramGateway(runtime.native, { application: runtime.application,
      model: { provider: 'approval-participation', model: 'controlled' }, participant: principal => ({ participant: 'human', actorId: principal.id }),
      access: service => service.registerPolicy('approval-test', { bindings: () => [local.id, peer.id].map(id => ({
        resource: { kind: 'session', id }, role: { name: 'approval-participant', resource: 'session', actions: [
          'reactivePrograms/open', 'reactivePrograms/view', 'reactivePrograms/resolveAction', 'reactivePrograms/startAction',
          'reactivePrograms/actionInvocation', 'reactivePrograms/actionResult', 'reactivePrograms/observeAction',
        ] },
      })) }), carrier: { onError: error => errors.push(error), async authenticate(request) {
        const actorId = request.headers.get('authorization')?.replace(/^Bearer /, '');
        if (!['bob', 'carol', 'eve'].includes(actorId)) throw new RemoteAuthenticationError(401);
        return { kind: 'approval-test', id: actorId };
      } },
    });
    const observer = await connect('bob');
    const observedPending = observer.session.module(layout.mounts.approval).viewResource('pending');
    assert.deepEqual(await observedPending.read(), []);
    const waiting = next(observedPending.values$, value => value.length === 1);
    void waiting.catch(() => {});
    planned = 'first';
    await local.submit({ requestId: 'first', content: '写入第一份文件', intent: 'question', delivery: 'followup', scope: null });
    const [request] = await waiting;
    assert.equal(request.callId, 'approval-first');
    await assert.rejects(access(join(root, 'first.txt')), { code: 'ENOENT' });
    await observer.client.dispose();
    const bob = await connect('bob'), carol = await connect('carol'), eve = await connect('eve');
    const bobModule = bob.session.module(layout.mounts.approval), carolModule = carol.session.module(layout.mounts.approval);
    assert.deepEqual(await bobModule.viewResource('pending').read(), [request], '观察者退出后原等待仍存在');
    await assert.rejects(answer(eve.session.module(layout.mounts.approval), local.id, request.transportId));
    const service = await runtime.application.service('approval-worker').open(local.id, program); handles.push(service);
    await assert.rejects(answer(service.module(layout.mounts.approval), local.id, request.transportId));
    const remotePeer = await bob.client.open(peer.id);
    await assert.rejects(answer(remotePeer.module(layout.mounts.approval), peer.id, request.transportId));
    await assert.rejects(answer(bobModule, peer.id, request.transportId));
    assert.equal(audit.filter(event => event.type === 'approval/decided').length, 0);
    assert.deepEqual(executions, []);
    const competing = [answer(bobModule, local.id, request.transportId), answer(carolModule, local.id, request.transportId)];
    for (const promise of competing) void promise.catch(() => {});
    await firstValueFrom(from(bothEntered.promise).pipe(timeout(5_000))); release.resolve();
    const outcomes = await Promise.allSettled(competing);
    assert.equal(outcomes.filter(value => value.status === 'fulfilled').length, 1);
    assert.deepEqual(outcomes.find(value => value.status === 'fulfilled').value, { status: 'submitted' });
    await local.whenIdle();
    assert.equal(await readFile(join(root, 'first.txt'), 'utf8'), 'approval-first');
    assert.deepEqual(executions, ['approval-first']);

    const secondWaiting = next(bobModule.viewResource('pending').values$, value => value.some(item => item.callId === 'approval-second'));
    void secondWaiting.catch(() => {});
    planned = 'second';
    await local.submit({ requestId: 'second', content: '写入第二份文件', intent: 'question', delivery: 'followup', scope: null });
    const [second] = await secondWaiting;
    await assert.rejects(access(join(root, 'second.txt')), { code: 'ENOENT' });
    await assert.rejects(answer(bobModule, local.id, request.transportId));
    assert.deepEqual(await bobModule.viewResource('pending').read(), [second], '旧transport不能消费后续真实等待');
    assert.deepEqual(await answer(bobModule, local.id, second.transportId), { status: 'submitted' });
    await local.whenIdle();
    assert.equal(await readFile(join(root, 'second.txt'), 'utf8'), 'approval-second');
    const asked = audit.filter(event => event.type === 'approval/asked'), decided = audit.filter(event => event.type === 'approval/decided');
    assert.deepEqual(asked.map(event => event.data.callId), ['approval-first', 'approval-second']);
    assert.deepEqual(decided.map(event => event.data.id), asked.map(event => event.data.id));
    assert.deepEqual(decided.map(event => event.data.outcome), ['allowed-once', 'allowed-once']);
    assert.deepEqual(executions, ['approval-first', 'approval-second']);
    assert.ok(offered.includes('approval_effect'));
    assert.equal(offered.some(name => name.includes('answer_approval')), false, '人工批准Action没有作为工具暴露给模型');
    assert.deepEqual(admitted.sort(), ['bob', 'bob', 'carol']);
    assert.deepEqual(errors, []);
  } finally {
    release.resolve();
    await Promise.all(clients.map(client => client.dispose()));
    for (const handle of handles) handle.release();
    removeAudit(); removeTool();
    try { await gateway?.close(); } finally { await runtime.dispose(); }
  }
});
