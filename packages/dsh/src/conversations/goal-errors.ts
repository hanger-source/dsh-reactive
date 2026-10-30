export class GoalRoundDriverUnavailableError extends Error {
  constructor(readonly reason: 'unavailable' | 'ambiguous' | 'changed') {
    super(`REACTIVE_GOAL_ROUND_DRIVER_${reason.toUpperCase()}`);
  }
}
