export type Observation<T> =
  | { status: 'pending'; generation: number }
  | { status: 'ready'; generation: number; value: T; observedAt: number }
  | { status: 'error'; generation: number; error: unknown };
