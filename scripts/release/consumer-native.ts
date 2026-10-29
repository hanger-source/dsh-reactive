import { createNativeJobs, type JobId, type NativeJob, type NativeJobs, type NativeSessionJobs } from '@hanger-source/dsh-reactive-dsh/jobs';
import { createNativeMcp, type NativeMcp } from '@hanger-source/dsh-reactive-dsh/mcp';
import { createNativeApproval, type NativeSessionApproval } from '@hanger-source/dsh-reactive-dsh/approval';
import { createNativeClock } from '@hanger-source/dsh-reactive-dsh/time';
import { createNativeSchedule, ScheduleId, ScheduleInputError, ScheduleLogError, type NativeSessionSchedule, type NativeScheduleTask,
  type NativeScheduleUpdate, type ScheduleUpdateResult, type ScheduleDeleteResult } from '@hanger-source/dsh-reactive-dsh/schedule';
import { createNativeTeam, type NativeSessionTeam, type NativeTeamTask, type NativeTeamTaskUpdate,
  type TeamTaskId, type TeamTaskView, type TeamMemberView, type CreateTeamTaskRequest,
  type UpdateTeamTaskRequest } from '@hanger-source/dsh-reactive-dsh/team';
import { createNativeFiles, type NativeSessionFiles, type NativeFileResource,
  type WorkspaceFileText } from '@hanger-source/dsh-reactive-dsh/files';
import { createNativeSkills, type NativeSessionSkills } from '@hanger-source/dsh-reactive-dsh/skills';
import { createNativeTools, type NativeSessionTools } from '@hanger-source/dsh-reactive-dsh/tools';
import { createNativeSubagents, SubagentError, type NativeSessionSubagents,
  type SubagentDescendantListEntry } from '@hanger-source/dsh-reactive-dsh/subagents';
import { combineResources, mapResource, shareResource, type Invocation, type Json } from '@hanger-source/dsh-reactive';
import { useResource } from '@hanger-source/dsh-reactive/program';
import type { ConversationHandle, NativeInvocation } from '@hanger-source/dsh-reactive-dsh';
import { ConsumerModuleProgram, readSelectedRevision, sessionControls, invocationReceipt } from './consumer-session.js';

export const nativeModuleRevision = (session: ConversationHandle<typeof ConsumerModuleProgram>, signal: AbortSignal) => readSelectedRevision(session, signal);
export const nativeControls = (session: ConversationHandle<typeof ConsumerModuleProgram>, signal: AbortSignal) => sessionControls(session, signal);

// 这些是正式npm子入口；只加载与编译，不为包检查安装Runtime或执行原生副作用。
export const entries = [createNativeJobs, createNativeMcp, createNativeApproval, createNativeClock,
  createNativeSchedule, createNativeTeam, createNativeFiles, createNativeSkills, createNativeTools, createNativeSubagents] as const;
export const task = (team: NativeSessionTeam, id: TeamTaskId): NativeTeamTask => team.task(id);
export const change = (task: NativeTeamTask, request: NativeTeamTaskUpdate): NativeInvocation<TeamTaskView> => task.update(request);
export const createTeamTask = (team: NativeSessionTeam, request: CreateTeamTaskRequest): NativeInvocation<TeamTaskView> => team.createTask(request);
export const changeTeamTask = (team: NativeSessionTeam, request: UpdateTeamTaskRequest): NativeInvocation<TeamTaskView> => team.updateTask(request);
export const changeSchedule = (task: NativeScheduleTask, request: NativeScheduleUpdate): NativeInvocation<ScheduleUpdateResult> => task.update(request);
export const deleteSchedule = (task: NativeScheduleTask): NativeInvocation<ScheduleDeleteResult> => task.delete();
export const scheduleTarget = (schedule: NativeSessionSchedule, storedId: string): NativeScheduleTask => schedule.task(ScheduleId(storedId));
export const scheduleFailure = (error: unknown) =>
  error instanceof ScheduleInputError || error instanceof ScheduleLogError ? error.code : undefined;
export const removeCollectedJob = (job: NativeJob, signal: AbortSignal): void => job.remove({ signal });
export const observeNativeWrite = <T>(invocation: NativeInvocation<T>, signal: AbortSignal) => invocationReceipt(invocation, signal);
export const useInvocationActivity = <T, O extends Json, Source>(invocation: Invocation<T, O, Source>) => useResource(invocation.activity);
export const filePage = (files: NativeSessionFiles, path: string): NativeFileResource<WorkspaceFileText> => files.text(path);
export const skillMaterial = (skills: NativeSessionSkills, files: NativeSessionFiles, name: string, path: string) =>
  combineResources({ catalog: skills.catalog, definition: skills.definition(name), file: files.text(path) });
export const nativeCatalogs = (tools: NativeSessionTools, skills: NativeSessionSkills) =>
  combineResources({ tools: tools.catalog, skills: skills.catalog });
export const nativeCollaboration = (subagents: NativeSessionSubagents, team: NativeSessionTeam) =>
  combineResources({ children: subagents.catalog, descendants: subagents.descendants, team: team.state, roster: team.roster });
export const nativeWorkspace = (subagents: NativeSessionSubagents, tools: NativeSessionTools,
  skills: NativeSessionSkills, team: NativeSessionTeam, signal: AbortSignal) => shareResource(combineResources({
    descendants: subagents.descendants, tools: tools.catalog, skills: skills.catalog, roster: team.roster,
  }), { signal });
export const readNativeWorkspace = (workspace: ReturnType<typeof nativeWorkspace>, signal: AbortSignal) => workspace.read(signal);
export const observeNativeWorkspace = (workspace: ReturnType<typeof nativeWorkspace>) =>
  workspace.observations$.pipe(mapResource(value => value.descendants));
export const useNativeWorkspace = (workspace: ReturnType<typeof nativeWorkspace>) => useResource(workspace);
export const readTeamRoster = (team: NativeSessionTeam, signal: AbortSignal): Promise<readonly TeamMemberView[]> => team.roster.read(signal);
export const runningTeamMembers = (team: NativeSessionTeam) =>
  team.roster.observations$.pipe(mapResource(members => members.filter(member => member.status === 'running')));
export const useNativeCollaboration = (subagents: NativeSessionSubagents, team: NativeSessionTeam) =>
  useResource(() => nativeCollaboration(subagents, team), [subagents, team]);
export const nativeDescendants = (subagents: NativeSessionSubagents, signal: AbortSignal) => subagents.readDescendants({ signal });
export const interruptNativeBranch = (subagents: NativeSessionSubagents, target: SubagentDescendantListEntry['id'], signal: AbortSignal): void =>
  subagents.interrupt(target, { signal });
export const nativeSubagentFailure = (error: unknown) => error instanceof SubagentError ? error.code : undefined;
export const toolPolicy = (tools: NativeSessionTools, approval: NativeSessionApproval) =>
  combineResources({ tools: tools.catalog, policy: approval.policy });
export const declaredNativeTools = (jobs: NativeJobs, mcp: NativeMcp) =>
  combineResources({ jobs: jobs.toolNames, mcp: mcp.toolNames, resources: mcp.resourceToolNames });
export const readMcpTools = (mcp: NativeMcp, signal: AbortSignal): Promise<readonly string[]> => mcp.toolNames.read(signal);
export const nativeState = (jobs: NativeSessionJobs, schedule: NativeSessionSchedule, team: NativeSessionTeam) =>
  combineResources({ jobs: jobs.state, schedule: schedule.state, team: team.state });
export const invalidateNativeJob = (job: NativeJob, signal: AbortSignal): void => job.invalidate({ signal });
export function taskState(jobs: NativeSessionJobs, schedule: NativeSessionSchedule, team: NativeSessionTeam,
  ids: { readonly job: JobId; readonly scheduled: ScheduleId; readonly task: TeamTaskId }) {
  const job: NativeJob = jobs.job(ids.job), scheduled: NativeScheduleTask = schedule.task(ids.scheduled),
    task: NativeTeamTask = team.task(ids.task);
  return { job, scheduled, task,
    resources: combineResources({ job: job.state, output: job.output, scheduled: scheduled.state, task: task.state }) };
}
