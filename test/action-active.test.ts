import { test } from 'node:test';
import assert from 'node:assert/strict';
import { firstValueFrom } from 'rxjs';
import { bindAction, defineAction, implementAction, z, type ActionExecutionContext } from '@hanger-source/dsh-reactive';

test('绑定当前调用集合包含并发准入与执行，晚订和退订不丢调用，release后原业务独立结算', async () => {
  const firstStarted = Promise.withResolvers<void>(), firstResult = Promise.withResolvers<string>();
  const secondAdmission = Promise.withResolvers<void>(), secondStarted = Promise.withResolvers<void>(), secondResult = Promise.withResolvers<string>();
  const execution = (invocationId: string): ActionExecutionContext => ({ invocationId, sessionId: 'merchant-session',
    source: { kind: 'participant', actorId: 'merchant-operator', participant: 'human', entrypoint: 'authenticated-sdk' },
    inputs: [], signal: new AbortController().signal, continuation: null });
  const effects: string[] = [];
  const definition = defineAction({ name: 'prepare', description: '处理当前商家本次资料', scope: z.object({ merchantId: z.string() }),
    input: z.enum(['first', 'second']), output: z.string() });
  const action = bindAction(implementAction(definition, { async execute(input, call) {
    effects.push(input);
    if (input === 'first') { firstStarted.resolve(); return firstResult.promise; }
    secondStarted.resolve();
    const result = await secondResult.promise;
    call.signal.throwIfAborted();
    return result;
  } }), { merchantId: 'merchant-a' }, { async admit(input) { if (input === 'second') await secondAdmission.promise; } });
  assert.deepEqual(await firstValueFrom(action.activeInvocations$), []);
  const first = action('first', execution('first'));
  await firstStarted.promise;
  const second = action('second', execution('second'));
  const late = await firstValueFrom(action.activeInvocations$);
  assert.deepEqual(late, [first, second]);
  assert.deepEqual(await firstValueFrom(second.activity$), { state: 'pending' });
  assert.deepEqual(effects, ['first']);
  const seen: string[][] = [];
  const subscription = action.activeInvocations$.subscribe(current => seen.push(current.map(invocation => invocation.id)));
  firstResult.resolve('first-recorded');
  assert.equal(await first.result, 'first-recorded');
  assert.deepEqual(await firstValueFrom(action.activeInvocations$), [second]);
  assert.deepEqual(late, [first, second], '此前交付的快照不会随当前集合删除成员');
  secondAdmission.resolve();
  await secondStarted.promise;
  action.release();
  await assert.rejects(action('first', execution('after-release')).result, /REACTIVE_ACTION_BINDING_RELEASED/u);
  assert.deepEqual(await firstValueFrom(action.activeInvocations$), [second]);
  subscription.unsubscribe();
  secondResult.resolve('second-recorded');
  assert.equal(await second.result, 'second-recorded');
  assert.deepEqual(await firstValueFrom(action.activeInvocations$), []);
  assert.deepEqual(effects, ['first', 'second']);
  assert.deepEqual(seen, [['first', 'second'], ['second']]);
});
