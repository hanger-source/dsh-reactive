import { combineResources, type ResourceSource } from '@hanger-source/dsh-reactive';
import { JobId, type JobView } from '@hanger-source/dsh-reactive-dsh/jobs/client';
import { TeamId, TeamTaskId, type TeamProjection } from '@hanger-source/dsh-reactive-dsh/team/client';
import type { ScheduleCatalogEntry, ScheduleId } from '@hanger-source/dsh-reactive-dsh/schedule/client';

/** 浏览器组合已有事实，不取得原生执行器、Session或安装owner。 */
export function nativeFacts(jobs: ResourceSource<readonly JobView[]>, team: ResourceSource<TeamProjection>,
  schedules: ResourceSource<readonly ScheduleCatalogEntry[]>) {
  return combineResources({ jobs, team, schedules });
}
export const nativeIds = (job: string, team: string, task: string) => ({ job: JobId(job), team: TeamId(team), task: TeamTaskId(task) });
export const scheduleIdentity = (record: ScheduleCatalogEntry): ScheduleId => record.id;
