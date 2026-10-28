import assert from 'node:assert/strict';
import { test } from 'node:test';
import { Scope } from '@hanger-source/dsh-reactive/reactivity';
import { nativeSource } from '@hanger-source/dsh-reactive-native-ui';

test('原生快照消费者观察跨owner派生失败及条件恢复，不缓存已失效值', () => {
  const owner = new Scope(), consumer = new Scope();
  const cell = owner.state(7), useOwner = consumer.state(true);
  const derived = consumer.computed(() => useOwner.value ? cell.value * 2 : 9);
  const source = nativeSource(derived), seen = [];
  const read = () => {
    try { seen.push({ value: source.getSnapshot() }); }
    catch (error) { seen.push({ error }); }
  };
  const stop = source.subscribe(read);
  try {
    assert.deepEqual(seen, [{ value: 14 }]);
    owner.dispose();
    assert.equal(seen.at(-1).error, owner.signal.reason);
    assert.throws(() => source.getSnapshot(), error => error === owner.signal.reason);
    useOwner.set(false);
    assert.deepEqual(seen.at(-1), { value: 9 });
    stop();
    useOwner.set(true);
    assert.deepEqual(seen.at(-1), { value: 9 });
  } finally { stop(); consumer.dispose(); owner.dispose(); }
});
