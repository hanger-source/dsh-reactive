import { test } from 'node:test';
import assert from 'node:assert/strict';
import { setImmediate } from 'node:timers/promises';
import { Component, createContext, createElement as h, useCallback, useContext, useEffect, useLayoutEffect, useState, useSyncExternalStore, type ReactNode } from 'react';
import { BehaviorSubject } from 'rxjs';
import { z } from 'zod';
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
