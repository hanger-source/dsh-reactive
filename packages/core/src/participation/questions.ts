import type { CancellationOptions } from '../conversations/protocol.ts';
import { mapResource } from '../resources/operators.ts';
import { shareResource } from '../resources/sharing.ts';
import type { ResourceReadable, ResourceReader, ResourceReaderOptions } from '../resources/reader.ts';
import type { HumanAnswer, HumanSnapshot, HumanQuestionSnapshot, HumanWaitFrame } from './contract.ts';

export interface HumanQuestionHandle<Options extends CancellationOptions = CancellationOptions> {
  readonly callId: string;
  readonly state: ResourceReader<HumanQuestionSnapshot>;
  answer(answer: HumanAnswer, options?: Options): Promise<boolean>;
  attachWait(options?: Options): AsyncIterable<HumanWaitFrame>;
}

export interface HumanQuestionControls<Options extends CancellationOptions> {
  answerQuestion(callId: string, answer: HumanAnswer, options?: Options): Promise<boolean>;
  attachWait(callId: string, options?: Options): AsyncIterable<HumanWaitFrame>;
}
export type HumanQuestionResolver<Options extends CancellationOptions = CancellationOptions> =
  (callId: string, options?: Options) => Promise<HumanQuestionHandle<Options>>;

function select(source: HumanSnapshot, callId: string): HumanQuestionSnapshot {
  return Object.freeze({
    active: source.questions.active.find(question => question.callId === callId),
    foreground: source.foreground.find(question => question.callId === callId),
    settled: source.questions.settled.find(question => question.callId === callId),
    replies: Object.freeze(source.replies.filter(reply => reply.questionCallId === callId)),
  });
}

/** 固定原来源和问题的可复用消费；控制仍经原端口重新准入，不创建问题、计时器或grant。 */
export function createHumanQuestionResolver<Options extends CancellationOptions>(source: ResourceReadable<HumanSnapshot>,
  controls: HumanQuestionControls<Options>, options: ResourceReaderOptions): HumanQuestionResolver<Options> {
  const owner = Object.freeze({ ...options });
  const handles = new Map<string, HumanQuestionHandle<Options>>();
  owner.signal.addEventListener('abort', () => handles.clear(), { once: true });
  return async (callId, readOptions) => {
    owner.signal.throwIfAborted();
    if (typeof callId !== 'string' || !callId.length) throw new Error('REACTIVE_HUMAN_QUESTION_ID_REQUIRED');
    const signal = readOptions?.signal ? AbortSignal.any([owner.signal, readOptions.signal]) : owner.signal;
    const current = select(await source.read(signal), callId);
    signal.throwIfAborted();
    if (!current.active && !current.foreground && !current.settled && !current.replies.length) throw new Error('REACTIVE_HUMAN_QUESTION_NOT_FOUND');
    let handle = handles.get(callId);
    if (!handle) {
      handle = Object.freeze({ callId,
        state: shareResource(source.observations$.pipe(mapResource(snapshot => select(snapshot, callId))), owner),
        answer(answer: HumanAnswer, options?: Options) {
          owner.signal.throwIfAborted(); options?.signal?.throwIfAborted();
          return controls.answerQuestion(callId, answer, options);
        },
        async *attachWait(options?: Options) {
          owner.signal.throwIfAborted(); options?.signal?.throwIfAborted();
          yield* controls.attachWait(callId, options);
        },
      });
      handles.set(callId, handle);
    }
    return handle;
  };
}
