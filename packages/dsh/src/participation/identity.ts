import type { CancellationOptions, SubmitOptions } from '@hanger-source/dsh-reactive';

/** 参与角色由可信取得入口固定；服务身份不能由逐次调用选项重写。 */
export type ParticipantIdentity = Readonly<{ participant: 'human' } | { participant: 'service'; actorId: string }>;
export type ServiceParticipantIdentity = Extract<ParticipantIdentity, { participant: 'service' }>;

export const humanParticipant: ParticipantIdentity = Object.freeze({ participant: 'human' });

export function serviceParticipant(actorId: string | undefined): ServiceParticipantIdentity {
    if (typeof actorId !== 'string' || !actorId.trim()) throw new Error('REACTIVE_SERVICE_ACTOR_REQUIRED');
    return Object.freeze({ participant: 'service', actorId });
}

export function assertParticipantOptions(identity: ParticipantIdentity, options: CancellationOptions): void {
    if (identity.participant === 'service' && 'actorId' in options) throw new Error('REACTIVE_SERVICE_ACTOR_IMMUTABLE');
}

export function participantActor(identity: ParticipantIdentity, options: SubmitOptions, humanActorId: string | undefined): string {
    assertParticipantOptions(identity, options);
    const actorId = identity.participant === 'service' ? identity.actorId : options.actorId ?? humanActorId;
    if (typeof actorId !== 'string' || !actorId.trim()) throw new Error('REACTIVE_PARTICIPANT_ACTOR_REQUIRED');
    return actorId;
}
