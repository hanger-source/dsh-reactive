import assert from 'node:assert/strict';
import { test } from 'node:test';
import { BehaviorSubject } from 'rxjs';
import { SlotCore } from '@deepseek-ai/dsh-client-ui-slots';
import { defineAction, readActionPresentation, toolActionBindingSchema, z } from '@hanger-source/dsh-reactive';
import { createNativeActionViews, defineNativeActionView } from '@hanger-source/dsh-reactive-native-ui';

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
