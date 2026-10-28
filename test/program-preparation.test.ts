import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setImmediate as immediate } from 'node:timers/promises';
import { Fragment, createElement as h, type ComponentType } from 'react';
import { BehaviorSubject, firstValueFrom, from, timeout } from 'rxjs';
import { defineProgram, defineSurface, z, type ProgramProps, type Workspace } from '@hanger-source/dsh-reactive';
import { implementProgram, useBeforeStep, useObservable } from '@hanger-source/dsh-reactive/program';
import { Conversation, ProgramInstance, Scope, createWorkspace } from '@hanger-source/dsh-reactive/kernel';

const contract = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) });
function programFor(Component: ComponentType<ProgramProps<typeof contract>>, directory: string) {
  const scope = new Scope();
  const workspace: Workspace = createWorkspace(scope, { id: 'preparation', directory });
  const sessionScope = scope.child();
  const sessionId = crypto.randomUUID();
  const conversation = new Conversation(sessionScope, { sessionId, workspaceId: workspace.id, environmentId: 'work', directory, parentSessionId: null },
    workspace, contract, { sessionId, sequence: -1, source: 'live', execution: 'idle', goal: null, requests: {}, waits: [] });
  const program = new ProgramInstance(sessionScope, implementProgram(contract, { Component }), conversation, workspace, {
    execution() { throw new Error('PREPARATION_MUST_NOT_EXECUTE_ACTION'); },
  });
  return { scope, program };
}
const wait = <T>(work: Promise<T>) => firstValueFrom(from(work).pipe(timeout(3_000)));

test('步骤前检查替换结束旧等待，步骤取消与Program失败也不被不配合的Promise阻塞', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-preparation-'));
  await writeFile(join(directory, 'a'), 'a'); await writeFile(join(directory, 'b'), 'b');
  const selected = new BehaviorSubject('a');
  const retired = Promise.withResolvers<void>(), entered = Promise.withResolvers<AbortSignal>();
  const nativeWait = Promise.withResolvers<void>(), nativeEntered = Promise.withResolvers<AbortSignal>();
  const failedWait = Promise.withResolvers<void>(), failedEntered = Promise.withResolvers<AbortSignal>();
  const nativeSignals: AbortSignal[] = [], checked: string[] = [];
  function Check({ target }: { target: string }) {
    useBeforeStep(async signal => {
      assert.equal(await readFile(join(directory, target), { encoding: 'utf8', signal }), target);
      if (target === 'a') { entered.resolve(signal); await retired.promise; }
      checked.push(target);
    }, [target]);
    return null;
  }
  function Root() {
    const current = useObservable(() => selected, []);
    return current.status === 'ready' ? h(Check, { target: current.value }) : null;
  }
  const { scope, program } = programFor(Root, directory);
  try {
    program.mount();
    const preparing = program.prepare(new AbortController().signal);
    const oldSignal = await wait(entered.promise);
    selected.next('b'); program.flush();
    assert.equal(oldSignal.aborted, true);
    await wait(preparing);
    assert.ok(checked.includes('b')); assert.equal(checked.includes('a'), false);
    retired.reject(new Error('RETIRED_CHECK_FAILED_LATE'));
    await immediate(); program.flush();

    const removeNative = program.beforeStep(signal => {
      nativeSignals.push(signal); nativeEntered.resolve(signal); return nativeWait.promise;
    });
    const step = new AbortController(), cancelled = new Error('NATIVE_STEP_CANCELLED');
    const pending = program.prepare(step.signal);
    const nativeSignal = await wait(nativeEntered.promise);
    step.abort(cancelled);
    await assert.rejects(wait(pending), error => error === cancelled);
    assert.equal(nativeSignal.aborted, true);
    nativeWait.resolve();
    await wait(program.prepare(new AbortController().signal));
    assert.equal(nativeSignals.at(-1)!.aborted, false, '步骤取消不永久撤销仍在声明中的检查');
    removeNative();

    program.beforeStep(signal => { failedEntered.resolve(signal); return failedWait.promise; });
    const parent = new AbortController(), failure = new Error('PROGRAM_PREPARATION_FAILED');
    const failing = program.prepare(parent.signal);
    const failedSignal = await wait(failedEntered.promise);
    program.fail(failure);
    await assert.rejects(wait(failing), error => error === failure);
    assert.equal(failedSignal.aborted, true); assert.equal(parent.signal.aborted, false);
  } finally { retired.resolve(); nativeWait.resolve(); failedWait.resolve(); selected.complete(); scope.dispose(); }
});

test('两个组件共享检查函数时，卸载其中一个仍执行另一组件的真实文件条件，当前失败继续拒绝准备', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-shared-check-'));
  const showLeft = new BehaviorSubject(true), required = join(directory, 'required-fact');
  const shared = async (signal: AbortSignal) => {
    assert.equal(await readFile(required, { encoding: 'utf8', signal }), 'ready');
  };
  function Guard() { useBeforeStep(shared, []); return null; }
  function Root() {
    const left = useObservable(() => showLeft, []);
    return h(Fragment, null, left.status === 'ready' && left.value ? h(Guard, { key: 'left' }) : null, h(Guard, { key: 'right' }));
  }
  const { scope, program } = programFor(Root, directory);
  try {
    program.mount();
    showLeft.next(false); program.flush();
    await assert.rejects(wait(program.prepare(new AbortController().signal)), { code: 'ENOENT' });
    await writeFile(required, 'ready');
    await wait(program.prepare(new AbortController().signal));
  } finally { showLeft.complete(); scope.dispose(); }
});
