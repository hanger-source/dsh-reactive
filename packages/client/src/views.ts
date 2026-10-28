import { decodeView, surfaceSlotDefinition, type Surface, type ResourceReader } from '@hanger-source/dsh-reactive';
import { decodeProgramFailure } from './contracts/errors.ts';
import { remoteResource } from './resource.ts';
import type { ClientScope } from './scope.ts';
import type { ProgramAddress, ProgramViewFrame } from './contracts/types.ts';

/** 原Surface决定地址和解码；构造槽不打开Gateway读取。 */
export function remoteView(owner: ClientScope, address: ProgramAddress, surface: Surface, name: string): ResourceReader<unknown> {
  const definition = surfaceSlotDefinition(surface, 'views', name);
  if (!definition) throw new Error('REACTIVE_VIEW_NOT_FOUND');
  return remoteResource<unknown, Extract<ProgramViewFrame, { type: 'baseline' }>, Exclude<ProgramViewFrame, { type: 'baseline' }>>(owner, {
    name: 'program-view:' + name,
    open: signal => owner.context.remote.reactivePrograms.view({ ...address, name }, signal),
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
