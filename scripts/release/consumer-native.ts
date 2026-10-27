import { createNativeJobs, type NativeJob, type NativeSessionJobs } from '@hanger-source/dsh-reactive-dsh/jobs';
import { createNativeMcp } from '@hanger-source/dsh-reactive-dsh/mcp';
import { createNativeApproval } from '@hanger-source/dsh-reactive-dsh/approval';
import { createNativeClock } from '@hanger-source/dsh-reactive-dsh/time';
import { createNativeSchedule, type NativeSessionSchedule, type NativeScheduleTask } from '@hanger-source/dsh-reactive-dsh/schedule';
import { createNativeTeam, type NativeSessionTeam, type NativeTeamTask, type NativeTeamTaskUpdate,
  type TeamTaskId } from '@hanger-source/dsh-reactive-dsh/team';
import { createNativeFiles, type NativeSessionFiles, type NativeFileResource,
  type WorkspaceFileText } from '@hanger-source/dsh-reactive-dsh/files';
import { createNativeSkills, type NativeSessionSkills } from '@hanger-source/dsh-reactive-dsh/skills';
import { createNativeTools, type NativeSessionTools } from '@hanger-source/dsh-reactive-dsh/tools';
import { combineResources } from '@hanger-source/dsh-reactive';

// 这些是正式npm子入口；只加载与编译，不为包检查安装Runtime或执行原生副作用。
export const entries = [createNativeJobs, createNativeMcp, createNativeApproval, createNativeClock,
  createNativeSchedule, createNativeTeam, createNativeFiles, createNativeSkills, createNativeTools] as const;
export const task = (team: NativeSessionTeam, id: TeamTaskId): Promise<NativeTeamTask> => team.task(id);
export const change = (task: NativeTeamTask, request: NativeTeamTaskUpdate) => task.update(request);
export const filePage = (files: NativeSessionFiles, path: string): NativeFileResource<WorkspaceFileText> => files.text(path);
export const skillMaterial = (skills: NativeSessionSkills, files: NativeSessionFiles, name: string, path: string) =>
  combineResources({ catalog: skills.catalog.observations$, definition: skills.definition(name).observations$,
    file: files.text(path).observations$ });
export const nativeCatalogs = (tools: NativeSessionTools, skills: NativeSessionSkills) =>
  combineResources({ tools: tools.catalog.observations$, skills: skills.catalog.observations$ });
export const nativeState = (jobs: NativeSessionJobs, schedule: NativeSessionSchedule, team: NativeSessionTeam) =>
  combineResources({ jobs: jobs.state.observations$, schedule: schedule.state.observations$, team: team.state.observations$ });
export const taskState = (job: NativeJob, scheduled: NativeScheduleTask, task: NativeTeamTask) =>
  combineResources({ job: job.state.observations$, output: job.output.observations$, scheduled: scheduled.state.observations$, task: task.state.observations$ });
