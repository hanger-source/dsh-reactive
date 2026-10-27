import type { Context } from '@deepseek-ai/cordis';
import { remoteResource, type RemoteResourceConnection } from './resource.ts';
import type { ProgramQuestions, ProgramQuestionsFrame } from './types.ts';

/** 只投影原生问答快照；回答和前台claim仍从会话控制端口重新准入。 */
export function remoteHuman(ctx: Context, sessionId: string, signal: AbortSignal): RemoteResourceConnection<ProgramQuestions> {
  return remoteResource<ProgramQuestions, Extract<ProgramQuestionsFrame, { type: 'baseline' }>, Exclude<ProgramQuestionsFrame, { type: 'baseline' }>>(ctx, signal, {
    name: 'program-questions:' + sessionId,
    open: signal => ctx.remote.reactivePrograms.questions({ sessionId }, signal),
    ended: () => new Error('REACTIVE_REMOTE_QUESTIONS_ENDED'),
    isSnapshot: (frame): frame is Extract<ProgramQuestionsFrame, { type: 'baseline' }> => frame.type === 'baseline',
    observation: (frame, generation) => ({ status: 'ready', generation, observedAt: Date.now(), value: frame.value }),
  });
}
