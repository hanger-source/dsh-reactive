import { captureHumanQuestionReference, type CancellationOptions, type HumanQuestionReference, type HumanSnapshot, type HumanWaitFrame, type ResourceReader } from '@hanger-source/dsh-reactive';
import { remoteResource } from './resource.ts';
import { revokeConversationAccess, type ClientScope } from './scope.ts';
import type { ProgramSessionAddress, ProgramQuestionsFrame } from './contracts/types.ts';
import { RemoteError } from '@deepseek-ai/dsh-typert-protocol';
import { decodeProgramFailure } from './contracts/errors.ts';

/** 只投影原生问答快照；回答和前台claim仍从会话控制端口重新准入。 */
export function remoteHuman(owner: ClientScope, address: ProgramSessionAddress): ResourceReader<HumanSnapshot> {
  const { sessionId } = address;
  return remoteResource<HumanSnapshot, Extract<ProgramQuestionsFrame, { type: 'baseline' }>, Exclude<ProgramQuestionsFrame, { type: 'baseline' }>>(owner, {
    name: 'program-questions:' + sessionId,
    open: signal => owner.context.remote.reactivePrograms.questions(address, signal),
    ended: () => new Error('REACTIVE_REMOTE_QUESTIONS_ENDED'),
    isSnapshot: (frame): frame is Extract<ProgramQuestionsFrame, { type: 'baseline' }> => frame.type === 'baseline',
    observation: (frame, generation) => {
      if (!Array.isArray(frame.value?.sourceConflicts)) return {
        status: 'error', generation, error: new Error('REACTIVE_REMOTE_HUMAN_SOURCE_EVIDENCE_INVALID'),
      };
      return { status: 'ready', generation, observedAt: Date.now(), value: {
        questions: frame.value.questions, foreground: frame.value.foreground, replies: frame.value.replies,
        sourceConflicts: frame.value.sourceConflicts,
      } };
    },
  });
}

/** 前台claim只持有一次原生物理流，不自动重连或重领，也不维护另一份问题状态。 */
export function createHumanWaits(owner: ClientScope, address: ProgramSessionAddress) {
  const remote = owner.context.remote;
  return {
    async *attach(callId: string, options: CancellationOptions = {}, expected?: HumanQuestionReference): AsyncIterable<HumanWaitFrame> {
      owner.assertActive();
      const reference = expected === undefined ? undefined : captureHumanQuestionReference(expected);
      const signal = options.signal ? AbortSignal.any([owner.signal, options.signal]) : owner.signal;
      signal.throwIfAborted();
      const lease = owner.acquire(() => {
        const stream = remote.reactivePrograms.attachWait({ ...address, callId, expected: reference }, signal);
        return { stream, iterator: stream[Symbol.asyncIterator]() };
      }, async ({ stream, iterator }) => {
        const failures: unknown[] = [];
        try { stream.dispose(); } catch (error) { failures.push(error); }
        try { await iterator.return?.(); } catch (error) { failures.push(error); }
        if (failures.length === 1) throw failures[0];
        if (failures.length) throw new AggregateError(failures, 'REACTIVE_REMOTE_HUMAN_WAIT_RELEASE_FAILED');
      }, 'program-human-wait:' + callId);
      const abort = () => { void lease.release().catch(() => {}); };
      signal.addEventListener('abort', abort, { once: true });
      try {
        signal.throwIfAborted();
        yield* { [Symbol.asyncIterator]: () => lease.value.iterator };
      } catch (error) { throw revokeConversationAccess(owner, error instanceof RemoteError ? decodeProgramFailure(error) : error); }
      finally { signal.removeEventListener('abort', abort); await lease.release(); }
    },
  };
}
