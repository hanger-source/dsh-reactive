import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { setImmediate as immediate } from 'node:timers/promises';
import { Fragment, createElement as h } from 'react';
import { BehaviorSubject, Observable, filter, firstValueFrom, timeout } from 'rxjs';
import { defineAction, implementAction, defineApplication, defineEnvironment, defineProgram, defineSurface, createWorkSelector, z,
  type Eligibility, type Observation } from '@hanger-source/dsh-reactive';
import { Context, Expose, Offer, implementProgram, useAction, useBeforeStep, useObservable, useProgramSession, useResource } from '@hanger-source/dsh-reactive/program';
import { boot, text, tool } from './native-support.ts';

test('同一资格源贯通模型准备、可选现场与稳定SDK合同，必需等待不阻断其他会话', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-program-eligibility-'));
  const state = new BehaviorSubject<Observation<Eligibility>>({ status: 'pending', generation: 0 });
  const supplementary = new BehaviorSubject(true);
  let connections = 0, models = 0, bodies = 0;
  const qualifications = new Observable<Observation<Eligibility>>(subscriber => {
    connections++; return state.subscribe(subscriber);
  });
  const preparing = Promise.withResolvers<void>(), modelTools: string[][] = [];
  const save = defineAction({ name: 'commit_eligible', description: '资料齐全后写入正式结果', scope: z.null(), input: z.null(), output: z.string() });
  const writer = implementAction(save, { async execute() { bodies++; await writeFile(join(root, 'accepted'), 'committed'); return 'committed'; } });
  const surface = defineSurface({ views: {}, actions: { save } });
  const contract = defineProgram({ input: z.null(), surface });
  function Gate({ required }: { readonly required: boolean }) {
    // 合法地观察自己的进展；观察更新不应把等价声明变成下一轮准备。
    useResource(useProgramSession().progress);
    const current = useObservable(supplementary);
    const action = useAction(writer, () => null, [], { eligibility: qualifications });
    useBeforeStep(() => { if (required) preparing.resolve(); }, [required]);
    return h(Fragment, null,
      required && current.status === 'ready' && current.value ? Context({ name: 'removable_facts',
        observation: { status: 'pending', generation: 0 }, render: () => null }) : null,
      Context({ name: 'independent_facts', value: { work: 'available' } }),
      Context({ name: 'qualified_facts', value: { approved: true }, eligibility: qualifications, required }),
      Offer({ action }), Expose({ surface, value: { actions: { save: action } } }));
  }
  const definition = defineApplication({ name: 'program-eligibility', admit: async () => {}, environments: [
    defineEnvironment({ id: 'optional', instructions: '只使用当前能力', program: implementProgram(contract, { Component: () => h(Gate, { required: false }) }) }),
    defineEnvironment({ id: 'required', instructions: '等待完整事实', program: implementProgram(contract, { Component: () => h(Gate, { required: true }) }) }),
  ] });
  const runtime = await boot(join(root, 'home'), definition, async function* (options) {
    models++; modelTools.push(options.tools?.map(tool => tool.name) ?? []);
    if (models === 2) yield* tool('commit_eligible', 'qualified-call', null);
    else yield* text('当前现场已读取');
  });
  const handles: { release(): void }[] = [];
  try {
    const optional = await runtime.host.create('optional', { workspaceId: runtime.workspace.workspaceId }); handles.push(optional);
    const required = await runtime.host.create('required', { workspaceId: runtime.workspace.workspaceId }); handles.push(required);
    assert.equal((await required.progress.read()).preparation, null);
    const captured = required.resolveAction('save', null);
    const settled = (session: typeof optional, id: string) => firstValueFrom(session.request(id).state.values$.pipe(
      filter(value => value.activity?.state === 'settled'), timeout(3_000)));
    let finished = settled(optional, 'optional-pending');
    await optional.submit({ requestId: 'optional-pending', content: '读取当前独立现场', intent: 'question', delivery: 'followup', scope: null });
    await finished;
    assert.equal(models, 1); assert.equal(modelTools[0]!.includes('commit_eligible'), false);
    assert.equal(connections, 2, '每个Program只连一次原冷来源，Context、Action与Offer借同源事实');
    finished = settled(required, 'required-pending');
    const waiting = firstValueFrom(required.progress.values$.pipe(filter(value => value.preparation?.waiting.some(wait =>
      wait.kind === 'context' && wait.name === 'qualified_facts') === true && value.preparation.waiting.some(wait =>
      wait.kind === 'context' && wait.name === 'removable_facts')), timeout(3_000)));
    await required.submit({ requestId: 'required-pending', content: '资料齐全后执行', intent: 'question', delivery: 'followup', scope: null });
    await preparing.promise;
    const blocked = await waiting, preparation = blocked.preparation!;
    assert.deepEqual(preparation.inputs.map(input => input.requestId), ['required-pending']);
    assert.equal((await required.request('required-pending').state.read()).preparation?.id, preparation.id);
    assert.equal((await required.request('another-work').state.read()).preparation, null);
    const work = createWorkSelector({ identify: origin => origin.kind === 'invocation' ? undefined : origin.requestId }).select('required-pending', { sessions: [{
      identity: required.identity, progress: blocked, human: await required.human.read(),
    }] }).sessions[0]!;
    assert.equal(work.preparation?.value.id, preparation.id);
    assert.ok(work.conditions.some(condition => condition.kind === 'preparation.waiting'));
    const removed = firstValueFrom(required.progress.values$.pipe(filter(value => value.preparation !== null
      && !value.preparation.waiting.some(wait => wait.kind === 'context' && wait.name === 'removable_facts')), timeout(3_000)));
    supplementary.next(false); await removed; await immediate();
    const remaining = (await required.progress.read()).preparation!;
    assert.equal(remaining.id, preparation.id);
    assert.deepEqual(remaining.waiting.filter(wait => wait.kind === 'context'), [{ kind: 'context', name: 'qualified_facts' }],
      '退出并列Context后先结束旧批次，当前准备只等待仍有效的B，不积累旧读取');
    assert.equal(models, 1, '必需资格pending时尚不能把现场交给模型');
    state.next({ status: 'ready', generation: 1, observedAt: Date.now(), value: { allowed: true } });
    await finished;
    assert.equal((await required.progress.read()).preparation, null);
    assert.equal(await readFile(join(root, 'accepted'), 'utf8'), 'committed'); assert.equal(bodies, 1);
    assert.equal(modelTools[1]!.includes('commit_eligible'), true);
    state.next({ status: 'ready', generation: 2, observedAt: Date.now(), value: { allowed: false, reason: '资料需要重新核对' } });
    assert.equal(required.resolveAction('save', null).bindingId, captured.bindingId);
    const manifest = await firstValueFrom(optional.actionManifest$.pipe(filter(entries => entries[0]?.eligibility?.status === 'ready'
      && !entries[0].eligibility.value.allowed), timeout(3_000)));
    assert.equal(manifest[0]!.enabled, false);
    finished = settled(optional, 'optional-denied');
    await optional.submit({ requestId: 'optional-denied', content: '继续读取独立现场', intent: 'question', delivery: 'followup', scope: null });
    await finished;
    assert.equal(bodies, 1); assert.equal(modelTools.at(-1)!.includes('commit_eligible'), false);
    assert.deepEqual(runtime.errors, []);
  } finally { handles.forEach(handle => handle.release()); await runtime.dispose(); state.complete(); supplementary.complete(); }
});
