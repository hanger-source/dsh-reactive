export type Observation<T> =
  | { status: 'pending'; generation: number }
  | { status: 'ready'; generation: number; value: T; observedAt: number }
  | { status: 'error'; generation: number; error: unknown };

/** @internal 公共观察入口只验证帧合同，业务值和观测时间由各自 owner 负责。 */
export function assertObservation<T>(observation: unknown): asserts observation is Observation<T> {
  if (observation !== null && typeof observation === 'object' &&
    'status' in observation && 'generation' in observation) {
    const { status, generation } = observation;
    if (typeof generation === 'number' && Number.isFinite(generation) &&
      (status === 'pending' || status === 'ready' && 'value' in observation || status === 'error' && 'error' in observation)) return;
  }
  throw new TypeError('REACTIVE_RESOURCE_OBSERVATION_INVALID');
}
