import { RemoteError, type Principal } from '@deepseek-ai/dsh-typert-protocol';

/** 由认证宿主确定的业务参与者；连接传输能力不授予此身份。 */
export interface ProgramParticipant {
  readonly participant: 'human' | 'service';
  readonly actorId: string;
}

/** @internal 只读取可信宿主映射，不从请求参数补写参与者。 */
export function resolveProgramParticipant(map: (principal: Principal) => ProgramParticipant, principal: Principal): ProgramParticipant {
  const value = map(principal);
  if (!value || (value.participant !== 'human' && value.participant !== 'service')
    || typeof value.actorId !== 'string' || !value.actorId.trim()) {
    throw new RemoteError('gateway/internal', 'Host participant mapping is invalid', {});
  }
  return Object.freeze({ participant: value.participant, actorId: value.actorId });
}
