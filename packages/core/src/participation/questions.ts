import type { CancellationOptions } from '../conversations/protocol.ts';
import { mapResource } from '../resources/operators.ts';
import { shareResource } from '../resources/sharing.ts';
import type { ResourceReadable, ResourceReader, ResourceReaderOptions } from '../resources/reader.ts';
import { HumanQuestionSourceConflictError, humanAnswerSchema, captureHumanQuestionReference, type HumanQuestionReference, type HumanQuestionOrigin, type HumanAnswer, type HumanSnapshot, type HumanQuestionSnapshot, type HumanWaitFrame } from './contract.ts';

export interface HumanQuestionHandle<Options extends CancellationOptions = CancellationOptions> {
  readonly callId: string;
  readonly origin: HumanQuestionOrigin;
  readonly state: ResourceReader<HumanQuestionSnapshot>;
  answer(answer: HumanAnswer, options?: Options): Promise<boolean>;
  attachWait(options?: Options): AsyncIterable<HumanWaitFrame>;
}

export interface HumanQuestionControls<Options extends CancellationOptions> {
  answerQuestion(callId: string, answer: HumanAnswer, options?: Options, expected?: HumanQuestionReference): Promise<boolean>;
  attachWait(callId: string, options?: Options, expected?: HumanQuestionReference): AsyncIterable<HumanWaitFrame>;
}
export type HumanQuestionResolver<Options extends CancellationOptions = CancellationOptions> =
  (callId: string, options?: Options) => Promise<HumanQuestionHandle<Options>>;

function freezeOrigin(value: HumanQuestionOrigin): HumanQuestionOrigin {
  const origin = structuredClone(value);
  const freeze = (value: unknown): void => {
    if (value === null || typeof value !== 'object') return;
    for (const child of Object.values(value)) freeze(child);
    Object.freeze(value);
  };
  freeze(origin);
  return origin;
}

function select(source: HumanSnapshot, callId: string, origin?: HumanQuestionOrigin): HumanQuestionSnapshot {
  const conflicts = source.sourceConflicts.filter(conflict => conflict.origin.callId === callId
    && (!origin || conflict.origin.sessionId === origin.sessionId && conflict.origin.callSequence === origin.callSequence));
  if (conflicts.length) throw new HumanQuestionSourceConflictError(conflicts);
  const matches = (candidate: HumanQuestionOrigin) => !origin || candidate.sessionId === origin.sessionId && candidate.callSequence === origin.callSequence;
  return Object.freeze({
    active: source.questions.active.find(question => question.callId === callId && matches(question.origin)),
    foreground: source.foreground.find(question => question.callId === callId && matches(question.origin)),
    settled: source.questions.settled.find(question => question.callId === callId && matches(question.origin)),
    replies: Object.freeze(source.replies.filter(reply => reply.questionCallId === callId && matches(reply.origin))),
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
    const origin = freezeOrigin((current.active ?? current.foreground ?? current.settled ?? current.replies[0])!.origin);
    const expected = captureHumanQuestionReference(origin);
    const key = JSON.stringify([origin.sessionId, origin.callSequence]);
    let handle = handles.get(key);
    if (!handle) {
      const assertCurrent = async (options?: Options) => {
        const signal = options?.signal ? AbortSignal.any([owner.signal, options.signal]) : owner.signal;
        const current = select(await source.read(signal), callId, origin);
        signal.throwIfAborted();
        if (!current.active && !current.foreground && !current.settled && !current.replies.length)
          throw new Error('REACTIVE_HUMAN_QUESTION_CHANGED');
      };
      handle = Object.freeze({ callId, origin,
        state: shareResource(source.observations$.pipe(mapResource(snapshot => select(snapshot, callId, origin))), owner),
        answer(answer: HumanAnswer, options?: Options) {
          owner.signal.throwIfAborted(); options?.signal?.throwIfAborted();
          const capturedAnswer = humanAnswerSchema.parse(answer);
          const capturedOptions = options === undefined ? undefined : { ...options };
          return assertCurrent(capturedOptions).then(() => controls.answerQuestion(callId, capturedAnswer, capturedOptions, expected));
        },
        async *attachWait(options?: Options) {
          owner.signal.throwIfAborted(); options?.signal?.throwIfAborted();
          const capturedOptions = options === undefined ? undefined : { ...options };
          await assertCurrent(capturedOptions);
          yield* controls.attachWait(callId, capturedOptions, expected);
        },
      });
      handles.set(key, handle);
    }
    return handle;
  };
}
