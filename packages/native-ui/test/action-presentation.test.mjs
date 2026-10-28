import assert from 'node:assert/strict';
import { test } from 'node:test';
import { BehaviorSubject, finalize } from 'rxjs';
import { SlotCore } from '@deepseek-ai/dsh-client-ui-slots';
import { defineAction, readActionPresentation, toolActionBindingSchema, z } from '@hanger-source/dsh-reactive';
import { createNativeActionViews, defineNativeActionView } from '@hanger-source/dsh-reactive-native-ui';

const bindingFor = (action, toolName) => toolActionBindingSchema.parse({ toolName, action: action.name, actionVersion: action.version,
  bindingId: 'binding-' + toolName, scope: null, modulePath: [], surfaceNames: ['save'], enabled: true });

test('真实原生Slot目录共存同名Action版本，历史结果保持原合同且不重复transform', () => {
  let parsed = 0;
  const first = defineAction({ name: 'save_record', description: '原合同', scope: z.null(), input: z.null(),
    output: z.string().transform(value => { parsed++; return Number(value); }) });
  const second = defineAction({ name: first.name, version: 2, description: '修订合同', scope: z.null(), input: z.null(),
    output: z.object({ confirmed: z.boolean() }) });
  const slots = new SlotCore();
  const root = slots.register({ name: 'root', children: { 'tool.call.toolview': { kind: 'keyed', scope: 'session' } } }, () => null);
  const seen = [], errors = [];
  const views = createNativeActionViews(slots, [
    defineNativeActionView(first, props => { seen.push(props.result?.output); return null; }),
    defineNativeActionView(second, props => { seen.push(props.result?.output); return null; }),
  ]);
  const subscription = views.errors$.subscribe(event => errors.push(event.error));
  const binding = (version, toolName) => toolActionBindingSchema.parse({
    toolName, action: first.name, actionVersion: version, bindingId: 'binding-' + version,
    scope: null, modulePath: [], surfaceNames: ['save'], enabled: true,
  });
  const manifest = new BehaviorSubject([binding(1, 'save_v1'), binding(2, 'save_v2')]);
  const stop = views.attach({ id: 'session', actionManifest$: manifest });
  const metadata = (actionVersion, output) => ({ reactiveAction: {
    version: 2, action: first.name, actionVersion, bindingId: 'binding-' + actionVersion, modulePath: [], scope: null, output,
  } });
  try {
    const entries = slots.entriesOfSlot('tool.call.toolview');
    assert.equal(entries.length, 2);
    const render = (toolName, meta) => {
      const entry = entries.find(entry => entry.options.key === toolName);
      entry.component({ phase: 'result', block: { meta } });
    };
    render('save_v1', metadata(1, 7));
    render('save_v2', metadata(2, { confirmed: true }));
    render('save_v1', metadata(2, { confirmed: true }));
    assert.deepEqual(seen, [7, { confirmed: true }, undefined]); assert.equal(parsed, 0);
    assert.equal(readActionPresentation(second, metadata(1, 7)), undefined);
    assert.throws(() => readActionPresentation(first, { reactiveAction: { version: 1, action: first.name,
      bindingId: 'old', modulePath: [], scope: null, output: 7 } }), '旧格式不能猜成Action版本1');
    manifest.next([]);
    assert.equal(slots.entriesOfSlot('tool.call.toolview').length, 2, '撤下声明仍保留本消费者的历史展示身份');
    const conflict = new BehaviorSubject([binding(2, 'save_v1')]);
    const stopConflict = views.attach({ id: 'conflict', actionManifest$: conflict });
    assert.equal(errors.length, 1, '同一真实工具名不得切换合同解释历史');
    assert.equal(slots.entriesOfSlot('tool.call.toolview').length, 2);
    stopConflict(); conflict.complete();
    stop(); assert.equal(slots.entriesOfSlot('tool.call.toolview').length, 0);
  } finally { stop(); subscription.unsubscribe(); views.dispose(); manifest.complete(); root(); }
});

test('SlotCore同步注册通知内abort或dispose立即撤下本批贡献，不继续注册后续工具', () => {
  for (const mode of ['abort', 'dispose']) {
    const action = defineAction({ name: 'save_registration', description: '同步退出注册', scope: z.null(), input: z.null(), output: z.null() });
    const slots = new SlotCore();
    const root = slots.register({ name: 'root', children: { 'tool.call.toolview': { kind: 'keyed', scope: 'session' } } }, () => null);
    const views = createNativeActionViews(slots, [defineNativeActionView(action, () => null)]);
    const signal = new AbortController(), mutations = [], errors = [];
    let ended = false, completed = 0;
    const observing = views.errors$.subscribe({ next: value => errors.push(value), complete: () => completed++ });
    const removeMutation = slots.onMutate(name => {
      if (name !== 'tool.call.toolview') return;
      const keys = slots.entriesOfSlot(name).map(entry => entry.options.key); mutations.push(keys);
      if (ended || keys[0] !== 'save_first') return;
      ended = true;
      if (mode === 'abort') signal.abort(new Error('ACTION_VIEW_OWNER_LEFT'));
      else views.dispose();
    });
    const manifest = new BehaviorSubject([bindingFor(action, 'save_first'), bindingFor(action, 'save_second')]);
    let stop;
    try {
      stop = views.attach({ id: 'session-' + mode, actionManifest$: manifest }, { signal: signal.signal });
      assert.deepEqual(mutations, [['save_first'], []]);
      assert.equal(slots.entriesOfSlot('tool.call.toolview').length, 0);
      manifest.next([bindingFor(action, 'save_after_exit')]);
      assert.equal(slots.entriesOfSlot('tool.call.toolview').length, 0);
      stop(); views.dispose();
      assert.deepEqual(errors, []); assert.equal(completed, 1);
    } finally { removeMutation(); stop?.(); observing.unsubscribe(); views.dispose(); manifest.complete(); root(); }
  }
});

test('同步注册重入共享同一pending slot，释放重入的新owner不会被旧清理删除', () => {
  const action = defineAction({ name: 'save_shared_registration', description: '共享真实注册', scope: z.null(), input: z.null(), output: z.null() });
  const slots = new SlotCore();
  const root = slots.register({ name: 'root', children: { 'tool.call.toolview': { kind: 'keyed', scope: 'session' } } }, () => null);
  const views = createNativeActionViews(slots, [defineNativeActionView(action, () => null)]);
  const first = new BehaviorSubject([bindingFor(action, 'save_shared')]), second = new BehaviorSubject([bindingFor(action, 'save_shared')]);
  const third = new BehaviorSubject([bindingFor(action, 'save_shared')]), signal = new AbortController();
  const errors = [], mutations = [];
  const observing = views.errors$.subscribe(event => errors.push(event.error));
  let stopFirst, stopSecond, stopThird, joined = false, reentered = false;
  const removeMutation = slots.onMutate(name => {
    if (name !== 'tool.call.toolview') return;
    const entries = slots.entriesOfSlot(name); mutations.push(entries);
    if (entries.length && !joined) {
      joined = true;
      stopSecond = views.attach({ id: 'second', actionManifest$: second });
      signal.abort();
    } else if (!entries.length && joined && !reentered) {
      reentered = true;
      stopThird = views.attach({ id: 'third', actionManifest$: third });
    }
  });
  try {
    stopFirst = views.attach({ id: 'first', actionManifest$: first }, { signal: signal.signal });
    const original = slots.entriesOfSlot('tool.call.toolview')[0]; assert.ok(original);
    assert.equal(mutations.length, 1, '第二消费者同步加入同一次正式register，不重复注册');
    stopFirst(); assert.equal(slots.entriesOfSlot('tool.call.toolview')[0], original);
    stopSecond();
    const renewed = slots.entriesOfSlot('tool.call.toolview')[0]; assert.ok(renewed);
    assert.notEqual(renewed, original, '正式释放先移除旧entry，mutation中新owner已取得另一真实注册');
    stopSecond(); assert.equal(slots.entriesOfSlot('tool.call.toolview')[0], renewed);
    stopThird(); assert.equal(slots.entriesOfSlot('tool.call.toolview').length, 0);
    assert.equal(mutations.length, 4); assert.deepEqual(errors, []);
  } finally { removeMutation(); stopFirst?.(); stopSecond?.(); stopThird?.(); observing.unsubscribe(); views.dispose(); first.complete(); second.complete(); third.complete(); root(); }
});

test('真实slot释放失败仍排空同owner其余贡献，显式调用与signal/source退出各归原错误接点', () => {
  for (const mode of ['stop', 'abort', 'abort-dispose', 'complete', 'dispose']) {
    const action = defineAction({ name: 'save_cleanup_registration', description: '排空真实贡献', scope: z.null(), input: z.null(), output: z.null() });
    const slots = new SlotCore();
    const root = slots.register({ name: 'root', children: { 'tool.call.toolview': { kind: 'keyed', scope: 'session' } } }, () => null);
    const views = createNativeActionViews(slots, [defineNativeActionView(action, () => null)]);
    const manifest = new BehaviorSubject([bindingFor(action, 'save_first'), bindingFor(action, 'save_second')]);
    const controller = new AbortController(), errors = [], failure = new Error('SLOT_RELEASE_MUTATION_FAILED');
    const observing = views.errors$.subscribe(event => errors.push(event));
    const source = mode === 'abort-dispose' ? manifest.pipe(finalize(() => views.dispose())) : manifest;
    const stop = views.attach({ id: 'cleanup-' + mode, actionManifest$: source }, { signal: controller.signal });
    const removeMutation = slots.onMutate(name => {
      if (name === 'tool.call.toolview' && slots.entriesOfSlot(name).map(entry => entry.options.key).join(',') === 'save_second') throw failure;
    });
    try {
      if (mode === 'stop' || mode === 'dispose') {
        assert.throws(() => mode === 'stop' ? stop() : views.dispose(), error => error === failure);
        assert.deepEqual(errors, []);
      } else {
        if (mode.startsWith('abort')) controller.abort(); else manifest.complete();
        assert.equal(errors.length, 1); assert.equal(errors[0].error, failure); assert.equal(errors[0].sessionId, 'cleanup-' + mode);
      }
      assert.equal(slots.entriesOfSlot('tool.call.toolview').length, 0, '首个真实disposer抛错不阻止其余slot释放');
      stop(); views.dispose();
    } finally { removeMutation(); stop(); observing.unsubscribe(); views.dispose(); manifest.complete(); root(); }
  }
});

test('register回执前dispose导致的迟到清理失败先交付errors再结束错误观察', () => {
  const action = defineAction({ name: 'save_pending_cleanup', description: '结算尚未返回的注册', scope: z.null(), input: z.null(), output: z.null() });
  const slots = new SlotCore();
  const root = slots.register({ name: 'root', children: { 'tool.call.toolview': { kind: 'keyed', scope: 'session' } } }, () => null);
  const views = createNativeActionViews(slots, [defineNativeActionView(action, () => null)]);
  const manifest = new BehaviorSubject([bindingFor(action, 'save_pending'), bindingFor(action, 'save_after_exit')]);
  const failure = new Error('PENDING_SLOT_RELEASE_FAILED'), seen = [];
  const observing = views.errors$.subscribe({ next: event => seen.push(event.error), complete: () => seen.push('complete') });
  let exiting = false;
  const removeMutation = slots.onMutate(name => {
    if (name !== 'tool.call.toolview') return;
    if (!exiting) { exiting = true; views.dispose(); }
    else if (!slots.entriesOfSlot(name).length) throw failure;
  });
  let stop;
  try {
    stop = views.attach({ id: 'pending-cleanup', actionManifest$: manifest });
    assert.deepEqual(seen, [failure, 'complete']);
    assert.equal(slots.entriesOfSlot('tool.call.toolview').length, 0);
    stop(); views.dispose();
  } finally { removeMutation(); stop?.(); observing.unsubscribe(); views.dispose(); manifest.complete(); root(); }
});
