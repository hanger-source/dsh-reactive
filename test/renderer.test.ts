import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { appendFile, mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setImmediate } from 'node:timers/promises';
import { Component, createContext, createElement as h, useCallback, useContext, useEffect, useLayoutEffect, useMemo, useState, useSyncExternalStore, type ReactNode } from 'react';
import { BehaviorSubject, Observable } from 'rxjs';
import { z } from 'zod';
import { defineAction, defineProgram, defineSurface, implementAction, view, type ActionExecutionContext } from '@hanger-source/dsh-reactive';
import { Conversation, ProgramInstance, Scope, createWorkspace } from '@hanger-source/dsh-reactive/kernel';
import { Expose, implementProgram, useAction } from '@hanger-source/dsh-reactive/program';
import { createDeclarationRenderer, declaration, type Frame } from '../packages/core/src/program/renderer.ts';

test('真实组件和 Observable 内部更新持续提交完整帧，key 保留状态与 callable，多个 root 共享 Context', async () => {
  const schema = z.object({ scope: z.string(), revision: z.number(), draft: z.number(), action: z.custom<() => void>(value => typeof value === 'function') });
  type Value = z.output<typeof schema>;
  const facts = new BehaviorSubject(1);
  const context = createContext('outside');
  const frames: Frame<Value>[] = [];
  const errors: unknown[] = [];
  const controller = new AbortController();
  let nextCommit: ((frame: Frame<Value>) => void) | undefined;
  let changeDraft: ((value: number) => void) | undefined;
  let effects = 0;
  let cleanups = 0;
  let actions = 0;
  const subscribe = (notify: () => void) => {
    const subscription = facts.subscribe(() => notify());
    return () => subscription.unsubscribe();
  };
  const getSnapshot = () => facts.value;
  function Work() {
    const scope = useContext(context);
    const revision = useSyncExternalStore(subscribe, getSnapshot);
    const [draft, setDraft] = useState(0);
    const action = useCallback(() => { actions++; }, []);
    useEffect(() => { effects++; changeDraft = setDraft; return () => { cleanups++; }; }, []);
    return declaration({ scope, revision, draft, action });
  }
  const renderer = createDeclarationRenderer({ decode: value => schema.parse(value), signal: controller.signal,
    onCommit(frame) { frames.push(frame); const resolve = nextCommit; nextCommit = undefined; resolve?.(frame); },
    onError: error => errors.push(error) });
  const secondFrames: Frame<string>[] = [];
  const second = createDeclarationRenderer({ decode: decodeString, onCommit: frame => secondFrames.push(frame), onError: error => errors.push(error) });
  function Other() { return declaration(useContext(context)); }
  const tree = (key: string) => h(context.Provider, { value: 'session-a' }, h(Work, { key }));
  const waitForCommit = () => { const completion = Promise.withResolvers<Frame<Value>>(); nextCommit = completion.resolve; return completion.promise; };
  try {
    renderer.render(tree('same'));
    second.render(h(context.Provider, { value: 'session-b' }, h(Other)));
    assert.equal(secondFrames.at(-1)!.declarations[0], 'session-b');
    const first = frames.at(-1)!.declarations[0]!;
    assert.equal(first.scope, 'session-a');
    assert.equal(actions, 0);
    assert.ok(Object.isFrozen(frames.at(-1)));
    assert.ok(Object.isFrozen(frames.at(-1)!.declarations));

    const stateChanged = waitForCommit();
    assert.ok(changeDraft); changeDraft(7);
    assert.equal((await stateChanged).declarations[0]!.draft, 7);
    const factsChanged = waitForCommit();
    facts.next(2);
    const current = (await factsChanged).declarations[0]!;
    assert.equal(current.revision, 2);
    assert.equal(current.scope, 'session-a');
    assert.equal(current.draft, 7);
    assert.equal(current.action, first.action);
    assert.equal(actions, 0);
    current.action(); assert.equal(actions, 1);
    assert.equal(effects, 1); assert.equal(cleanups, 0);

    renderer.render(tree('replacement'));
    assert.equal(frames.at(-1)!.declarations[0]!.draft, 0);
    assert.notEqual(frames.at(-1)!.declarations[0]!.action, first.action);
    assert.equal(cleanups, 1);
    renderer.unmount();
    assert.deepEqual(frames.at(-1)!.declarations, []);
    assert.equal(facts.observed, false);
    renderer.render(tree('reopened'));
    const beforeAbort = frames.length;
    controller.abort();
    assert.equal(frames.length, beforeAbort);
    assert.equal(facts.observed, false);
    assert.equal(effects, cleanups);
    assert.throws(() => renderer.flush(), /REACTIVE_RENDERER_DISPOSED/u);
    await setImmediate();
    assert.deepEqual(errors, []);
    assert.ok(frames.every((frame, index) => index === 0 || frame.revision > frames[index - 1]!.revision));
  } finally { renderer.dispose(); second.dispose(); }
});

test('LayoutEffect连续提交后只连接当前目标的真实文件来源，公开Action不借用已退出代际', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-commit-lifetime-'));
  await writeFile(join(directory, 'A'), 'retired-fact');
  await writeFile(join(directory, 'B'), 'current-fact');
  const saved = join(directory, 'saved');
  const owner = new Scope(), scope = owner.child();
  const workspace = createWorkspace(owner, { id: 'commit-lifetime', directory });
  const save = defineAction({ name: 'save_target', description: '保存当前目标',
    scope: z.object({ target: z.enum(['A', 'B']) }), input: z.null(), output: z.string() });
  const actionOwner = implementAction(save, { async execute(_input, call) {
    await appendFile(saved, call.scope.target + '\n');
    return call.scope.target;
  } });
  const surface = defineSurface({ views: { current: view(z.string()) }, actions: { save } });
  const definition = defineProgram({ input: z.null(), surface });
  const identity = { sessionId: 'commit-lifetime', workspaceId: workspace.id, environmentId: 'work', directory, parentSessionId: null };
  const conversation = new Conversation(scope, identity, workspace, definition, { sessionId: identity.sessionId,
    sequence: -1, source: 'live', execution: 'idle', inputs: [], calls: [], replies: [], goal: null, requests: {}, waits: [], operationFollows: [], failures: [] });
  const execution = (): ActionExecutionContext => ({ invocationId: crypto.randomUUID(), sessionId: identity.sessionId,
    source: { kind: 'participant', participant: 'service', actorId: 'surface-consumer', entrypoint: 'program' },
    inputs: [], continuation: null, signal: new AbortController().signal });
  const mounted = new Set<string>(), connected = new Set<string>(), reads: string[] = [];
  const calls: Promise<{ value: unknown } | { error: unknown }>[] = [];
  let program: ProgramInstance<typeof definition>;
  function Worker({ target }: { target: 'A' | 'B' }) {
    const action = useAction(actionOwner, () => ({ target }), [target]);
    useLayoutEffect(() => {
      mounted.add(target);
      return () => { mounted.delete(target); };
    }, [target]);
    const source$ = useMemo(() => new Observable<string>(subscriber => {
      connected.add(target);
      const fact = readFileSync(join(directory, target), 'utf8');
      reads.push(fact);
      const exposed = program.outputs.actions.peek().save!;
      const invocation = program.invoke(exposed, null, execution(), () => program.outputs.actions.peek().save === exposed);
      calls.push(invocation.result.then(value => ({ value }), error => ({ error })));
      subscriber.next(fact);
      return () => { connected.delete(target); };
    }), [target]);
    return Expose({ surface, value: { views: { current: source$ }, actions: { save: action } } });
  }
  function Root() {
    const [target, select] = useState<'A' | 'B'>('A');
    useLayoutEffect(() => { select('B'); }, []);
    return h(Worker, { target });
  }
  program = new ProgramInstance(scope, implementProgram(definition, { Component: Root }), conversation, workspace, { execution });
  try {
    program.mount();
    const results = await Promise.all(calls);
    assert.deepEqual(reads, ['current-fact'], '已经卸载的A不得因历史帧发布而重新读取文件');
    assert.deepEqual([...mounted], ['B']);
    assert.deepEqual([...connected], ['B']);
    assert.equal(await program.outputs.views.current.read(), 'current-fact');
    assert.deepEqual(results, [{ value: 'B' }], '来源同步消费公开Action时，必须取得当前且可受理的绑定');
    assert.equal(await readFile(saved, 'utf8'), 'B\n');
  } finally { owner.dispose(); await owner.closed; }
  assert.equal(connected.size, 0);
});

test('发布回调重入卸载并连续挂载时真实清理旧树，仍交付可执行的当前声明', () => {
  const active = new Set<string>(), cleaned: string[] = [], executed: string[] = [], errors: unknown[] = [];
  function Work({ name }: { name: string }) {
    useLayoutEffect(() => {
      active.add(name);
      return () => { active.delete(name); cleaned.push(name); };
    }, [name]);
    return declaration({ name, run() {
      assert.ok(active.has(name), '已经退出的声明不能重新交给消费者执行');
      executed.push(name);
    } });
  }
  const schema = z.object({ name: z.string(), run: z.custom<() => void>(value => typeof value === 'function') });
  const renderer = createDeclarationRenderer({ decode: value => schema.parse(value), onError: error => errors.push(error),
    onCommit(frame) {
      for (const command of frame.declarations) {
        command.run();
        if (command.name === 'initial') {
          renderer.unmount();
          renderer.render(h(Work, { name: 'superseded' }));
          renderer.render(h(Work, { name: 'current' }));
        }
      }
    },
  });
  try {
    renderer.render(h(Work, { name: 'initial' }));
    assert.deepEqual(errors, []);
    assert.deepEqual(cleaned, ['initial', 'superseded']);
    assert.deepEqual(executed, ['initial', 'current']);
    assert.deepEqual([...active], ['current']);
  } finally { renderer.dispose(); }
  assert.equal(active.size, 0);
});

class Boundary extends Component<{ children: ReactNode }, { failed: boolean }> {
  state = { failed: false };
  static getDerivedStateFromError() { return { failed: true }; }
  render() { return this.state.failed ? declaration('error-boundary-result') : this.props.children; }
}

test('渲染、commit Effect、错误边界和声明解码失败都阻断发布，错误树的空帧不会冒充成功', async () => {
  const cases: { name: string; element(error: Error): ReactNode }[] = [
    { name: 'render', element(error) { function Broken(): never { throw error; } return h(Broken); } },
    { name: 'layout-effect', element(error) {
      function Broken() { useLayoutEffect(() => { throw error; }, []); return declaration('unpublished'); }
      return h(Broken);
    } },
    { name: 'passive-effect', element(error) {
      function Broken() { useEffect(() => { throw error; }, []); return declaration('unpublished'); }
      return h(Broken);
    } },
    { name: 'caught-error', element(error) { function Broken(): never { throw error; } return h(Boundary, { children: h(Broken) }); } },
    { name: 'decode', element() { return declaration(42); } },
  ];
  for (const scenario of cases) {
    const expected = new Error('renderer-' + scenario.name);
    const errors: unknown[] = [];
    const frames: Frame<string>[] = [];
    const renderer = createDeclarationRenderer({
      decode(value) { if (typeof value !== 'string') throw expected; return value; },
      onCommit: frame => frames.push(frame), onError: error => errors.push(error),
    });
    try {
      renderer.render(declaration('accepted-before-error'));
      assert.throws(() => renderer.render(scenario.element(expected)), error => error === expected);
      await setImmediate();
      assert.deepEqual(errors, [expected], scenario.name);
      assert.deepEqual(frames.map(frame => frame.declarations), [['accepted-before-error']], scenario.name);
      assert.throws(() => renderer.render(declaration('forbidden-after-error')), error => error === expected);
      assert.throws(() => renderer.flush(), error => error === expected);
    } finally { renderer.dispose(); }
  }
});

test('异步内部 commit 的错误通知可以 abort owner，清理不重入 React，也不发布失败帧', async () => {
  const source = new BehaviorSubject(false);
  const controller = new AbortController();
  const expected = new Error('async-render-failed');
  const failure = Promise.withResolvers<unknown>();
  const frames: Frame<string>[] = [];
  const subscribe = (notify: () => void) => { const subscription = source.subscribe(() => notify()); return () => subscription.unsubscribe(); };
  function Work() {
    const broken = useSyncExternalStore(subscribe, () => source.value);
    if (broken) throw expected;
    return declaration('last-valid-frame');
  }
  const renderer = createDeclarationRenderer({ decode: decodeString, signal: controller.signal,
    onCommit: frame => frames.push(frame), onError(error) { controller.abort(); failure.resolve(error); } });
  try {
    renderer.render(h(Work));
    source.next(true);
    assert.equal(await failure.promise, expected);
    await setImmediate();
    assert.deepEqual(frames.map(frame => frame.declarations), [['last-valid-frame']]);
    assert.equal(source.observed, false);
    assert.throws(() => renderer.render(declaration('not-admitted')), /REACTIVE_RENDERER_DISPOSED/u);
  } finally { renderer.dispose(); }
});

function decodeString(value: unknown) { if (typeof value !== 'string') throw new TypeError('STRING_DECLARATION_REQUIRED'); return value; }
