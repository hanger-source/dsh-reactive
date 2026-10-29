import { createProgramClient } from '@hanger-source/dsh-reactive-client';
import { program } from './browser-request-contracts.mjs';
import { expandAssistantStream } from '@deepseek-ai/dsh-llm/assistant-stream';
import { filter, firstValueFrom, timeout } from 'rxjs';

const button = document.querySelector('#requests'), output = document.querySelector('#status');
const next = (source, predicate) => firstValueFrom(source.pipe(filter(predicate), timeout({ first: 8000 })));
const text = snapshot => snapshot.attempts.flatMap(attempt => expandAssistantStream(attempt.stream))
  .filter(member => member.chunk.type === 'text-delta').map(member => member.chunk.text).join('');
const check = (value, message) => { if (!value) throw new Error(message); };
const submittedAt = '2026-10-27T00:00:07.000Z';
output.textContent = '可以验证真实请求进展';
button.onclick = async () => {
  button.disabled = true;
  let client, journal, result;
  const failures = [];
  const cleanup = async dispose => { try { await dispose(); } catch (error) { failures.push(error); } };
  const interaction = new AbortController();
  const subscriptions = [];
  try {
    client = await createProgramClient(program, { baseUrl: location.origin });
    const { sessionId } = await (await fetch('/request-info')).json();
    const session = await client.open(sessionId), request = session.request('browser-request');
    const initial = await next(request.state$, value => value.status === 'ready');
    check(initial.value.source === 'durable' && initial.value.activity === null && initial.value.attempts.length === 0, 'COLD_REQUEST_BASELINE');
    journal = await session.openEvents({ publish() {}, failed: error => { throw error; } });
    const cold = await (await fetch('/proof')).json(); check(!cold.agentPresent && cold.models === 0, 'COLD_JOURNAL_ACTIVATED_AGENT');
    await journal.dispose(); journal = undefined;
    await client.resume(sessionId);
    const raw = [];
    subscriptions.push(session.progressFrames$.subscribe(value => raw.push(value)));
    await session.submit({ requestId: 'browser-request', content: '核验本次浏览器请求', delivery: 'followup', intent: 'question', scope: { at: submittedAt } }, { signal: interaction.signal });
    await next(request.state$, value => value.status === 'ready' && text(value.value) === '浏览器当前前缀');
    const late = await client.open(sessionId), lateRequest = late.request('browser-request'), lateEvents = [];
    subscriptions.push(lateRequest.events$.subscribe(value => lateEvents.push(value)));
    const opened = await next(lateRequest.state$, value => value.status === 'ready' && text(value.value) === '浏览器当前前缀');
    check(lateEvents.length === 0, 'LATE_PREFIX_REPLAYED_AS_EVENTS');
    const attempt = opened.value.attempts[0];
    check(attempt.inputs.some(input => input.requestId === 'browser-request' && input.actorId === 'browser-request-participant'
      && input.sourceSessionId === sessionId && input.scope.at === submittedAt), 'REQUEST_RAW_INPUT_IDENTITY');
    const delta = next(lateRequest.events$, value => value.kind === 'assistant.stream' && value.frame.phase === 'text' && value.frame.text === '与真实后续增量');
    await fetch('/request-next', { method: 'POST' });
    await Promise.all([next(lateRequest.state$, value => value.status === 'ready' && text(value.value) === '浏览器当前前缀与真实后续增量'), delta]);
    check(lateEvents.some(value => value.kind === 'assistant.stream' && value.frame.phase === 'text' && value.frame.text === '与真实后续增量'), 'LIVE_DELTA_MISSING');
    const message = next(request.events$, value => value.kind === 'assistant.message');
    await fetch('/request-finish', { method: 'POST' });
    const committed = await message;
    check(committed.inputs.some(input => input.requestId === 'browser-request' && input.actorId === 'browser-request-participant'
      && input.sourceSessionId === sessionId && input.scope.at instanceof Date && input.scope.at.toISOString() === submittedAt), 'REQUEST_TYPED_INPUT_IDENTITY');
    const settled = await next(request.state$, value => value.status === 'ready' && value.value.activity?.state === 'settled');
    check(settled.value.activity.reply === committed.text && settled.value.attempts.length === 0, 'REQUEST_COMMIT');
    check(raw.some(value => value.frame.type === 'chunk' && value.frame.chunk.type === 'block-start')
      && raw.some(value => value.frame.type === 'chunk' && value.frame.chunk.type === 'block-end'), 'LOSSLESS_RAW_FRAMES');
    const proof = await (await fetch('/proof')).json(); check(proof.models === 1 && proof.errors.length === 0, 'HOST_EXECUTION_COUNT');
    for (const kind of ['local', 'raw']) check(proof.facts[kind].some(input => input.requestId === 'browser-request'
      && input.sourceSessionId === sessionId && input.actorId === 'browser-request-participant'
      && input.domainDate === (kind === 'local') && input.at === submittedAt), 'HOST_INPUT_' + kind);
    result = { passed: true, coldAgent: cold.agentPresent, models: proof.models,
      latePrefix: text(opened.value), reply: committed.text, actorId: attempt.inputs[0].actorId,
      sourceSessionId: attempt.inputs[0].sourceSessionId, typedInput: committed.inputs[0].scope.at.toISOString(),
      rawChunkTypes: raw.filter(value => value.frame.type === 'chunk').map(value => value.frame.chunk.type) };
  } catch (error) { failures.push(error); }
  finally {
    await cleanup(() => interaction.abort());
    for (const subscription of subscriptions) await cleanup(() => subscription.unsubscribe());
    await cleanup(() => journal?.dispose());
    await cleanup(() => client?.dispose());
  }
  if (failures.length) {
    const error = failures.length === 1 ? failures[0] : new AggregateError(failures, 'BROWSER_REQUEST_CONSUMER_FAILED');
    const describe = error => ({ error: String(error), stack: error?.stack,
      ...(error instanceof AggregateError ? { errors: error.errors.map(describe) } : {}) });
    console.error(error);
    result = { passed: false, ...describe(error), errors: failures.map(describe) };
  }
  output.textContent = JSON.stringify(result);
};
