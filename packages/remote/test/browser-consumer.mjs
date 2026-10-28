import { createProgramClient } from '@hanger-source/dsh-reactive-client';
import { program } from './browser-contracts.mjs';
import { filter, firstValueFrom, timeout } from 'rxjs';

const output = document.querySelector('output');
document.querySelector('button').addEventListener('click', async () => {
  let client;
  const interaction = new AbortController(), options = { signal: interaction.signal };
  try {
    client = await createProgramClient(program, { baseUrl: location.origin });
    const { workspaceId, sessionId } = await fetch('/setup').then(response => response.json());
    const session = await client.create('work', { workspaceId, sessionId });
    const initial = await session.readView('current', options);
    const check = await session.resolveAction('check', null, options);
    const invocation = await check({ amount: '7' }, options);
    const receipt = await invocation.result;
    const operation = await invocation.operation;
    const settled = await operation.settled(options);
    if ((await firstValueFrom(invocation.activity$.pipe(filter(value => value.state === 'returned'), timeout(8000)))).state !== 'returned'
      || (await invocation.activity.read()).state !== 'returned') throw new Error('BROWSER_INVOCATION_ACTIVITY_LOST');
    const updated = await session.readView('current', options);
    const proof = await fetch('/proof').then(response => response.json());
    if (!(initial instanceof Date) || !(receipt.at instanceof Date) || !(settled.at instanceof Date)
      || receipt.at.getTime() !== updated.getTime() || proof.executions !== 1 || proof.parsedAmount !== 7 || proof.queries !== 1) throw new Error('BROWSER_BOUNDARY_FAILED');
    const rejected = await check({ amount: '13' }, options);
    const resultError = await rejected.result.then(() => { throw new Error('EXPECTED_BUSINESS_FAILURE'); }, error => error);
    const activity = await firstValueFrom(rejected.activity$.pipe(filter(value => value.state === 'failed'), timeout({ first: 8000 })));
    const currentFailure = await rejected.activity.read();
    const failure = error => ({ code: error.code, message: error.message, details: error.details });
    const expected = { code: 'test/revision-conflict', message: '订单版本已变化', details: { orderId: 'order-7', expectedRevision: 7, actualRevision: 8 } };
    const matches = error => error.code === expected.code && error.message === expected.message
      && Object.keys(error.details).length === 3 && Object.entries(expected.details).every(([key, value]) => error.details[key] === value);
    if (!matches(resultError) || !matches(activity.error) || rejected.activity.peek().status !== 'ready'
      || currentFailure.state !== 'failed' || !matches(currentFailure.error)) throw new Error('BROWSER_FAILURE_DETAILS_LOST');
    const failedProof = await fetch('/proof').then(response => response.json());
    if (failedProof.executions !== 2 || failedProof.parsedAmount !== 13 || failedProof.queries !== 1) throw new Error('BROWSER_FAILURE_EXECUTION_CHANGED');
    output.textContent = JSON.stringify({ passed: true, source: invocation.source, initial: initial.toISOString(),
      receipt: receipt.at.toISOString(), settled: settled.at.toISOString(), ...proof, failure: failure(resultError), failedProof });
    await session.release();
  } catch (error) { output.textContent = JSON.stringify({ passed: false, error: String(error), stack: error.stack }); }
  finally { interaction.abort(); await client?.dispose(); }
});
output.textContent = '可以执行真实远程调用';
