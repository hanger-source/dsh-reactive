import { createReaction, type Scope } from '@hanger-source/dsh-reactive';
import { EMPTY, exhaustMap, mergeMap, of, throwError, timer, withLatestFrom } from 'rxjs';
import type { ServiceConversation } from '@hanger-source/dsh-reactive-dsh';
import { InspectionProgram, InspectionReport, SiteReading, type InspectionTarget } from './contracts.ts';
import type { InspectionCapabilities } from './services.ts';

export interface InspectionParticipant {
  readonly session: ServiceConversation<typeof InspectionProgram>;
  readonly target: InspectionTarget;
}

/**
 * 进程内周期感知直接用 RxJS timer；持久提醒由独立 Schedule owner 管理。
 * 普通巡检调用 Action，仅异常报告显式投递对话，不为每个时钟创建会话或 Goal。
 */
export function startFleetInspection(scope: Scope, participants: readonly InspectionParticipant[],
  capabilities: InspectionCapabilities, periodMs: number) {
  if (!Number.isSafeInteger(periodMs) || periodMs <= 0 || periodMs > 2_147_483_647) throw new Error('INSPECTION_PERIOD_REQUIRED');
  // 先取得每个确切绑定；一个目标无效时不留下已经开始运行的半批巡检。
  const sessions = new Set<string>();
  const selected = participants.map(({ session, target }) => {
    if (sessions.has(session.id)) throw new Error('INSPECTION_PARTICIPANT_DUPLICATE');
    sessions.add(session.id);
    if (session.identity.workspaceId !== target.workspaceId) throw new Error('INSPECTION_WORKSPACE_MISMATCH');
    const inspect = session.resolveAction('inspect', target);
    return { session, target: Object.freeze({ ...target }), inspect };
  });
  const rules = selected.map(({ session, target, inspect }) => createReaction(scope, {
    name: `inspection:${session.id}`, event: SiteReading, result: InspectionReport,
    observe: () => timer(0, periodMs).pipe(withLatestFrom(capabilities.readings.observations$),
      mergeMap(([, observation]) => {
        if (observation.status === 'error') return throwError(() => observation.error);
        if (observation.status === 'pending') return EMPTY;
        const reading = observation.value.find(current => current.siteId === target.siteId);
        return reading ? of(reading) : throwError(() => new Error('INSPECTION_SITE_NOT_FOUND'));
      })),
    operator: exhaustMap,
    async execute(reading) {
      const report = await inspect({ expectedRevision: reading.revision }).result;
      if (report.needsReview) await session.submit({ requestId: `inspection:${report.inspectionId}`, scope: target,
        intent: 'followup', delivery: 'followup', content: JSON.stringify(report) });
      return report;
    },
  }));
  return rules.map(rule => rule.start());
}
