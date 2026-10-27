import type { Context } from '@deepseek-ai/cordis';
import { decodeView, surfaceSlotDefinition, type Surface } from '@hanger-source/dsh-reactive';
import { decodeProgramFailure } from './errors.ts';
import { remoteResource, type RemoteResourceConnection } from './resource.ts';
import type { ProgramViewFrame } from './types.ts';

/** 原Surface决定地址和解码；构造槽不打开Gateway读取。 */
export function remoteView(ctx: Context, sessionId: string, surface: Surface, name: string,
  signal: AbortSignal): RemoteResourceConnection<unknown> {
  const definition = surfaceSlotDefinition(surface, 'views', name);
  if (!definition) throw new Error('REACTIVE_VIEW_NOT_FOUND');
  return remoteResource<unknown, Extract<ProgramViewFrame, { type: 'baseline' }>, Exclude<ProgramViewFrame, { type: 'baseline' }>>(ctx, signal, {
    name: 'program-view:' + name,
    open: signal => ctx.remote.reactivePrograms.view({ sessionId, name }, signal),
    ended: () => new Error('REACTIVE_REMOTE_VIEW_ENDED'),
    isSnapshot: (frame): frame is Extract<ProgramViewFrame, { type: 'baseline' }> => frame.type === 'baseline',
    observation(frame) {
      const observation = frame.observation;
      if (observation.status === 'ready') return { ...observation, value: decodeView(definition, observation.value) };
      if (observation.status === 'error') return { ...observation, error: decodeProgramFailure(observation.error) };
      return observation;
    },
  });
}
