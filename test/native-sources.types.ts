import type { ProgramContract, ProgramSession, Invocation, InvocationActivity, Json, FileUploadInvocation, FileUploadValue } from '@hanger-source/dsh-reactive';
import type { NativeSessionSource, NativeInvocation } from '@hanger-source/dsh-reactive-dsh';
import type { NativeJob, NativeJobs } from '@hanger-source/dsh-reactive-dsh/jobs';
import type { NativeSchedule, NativeScheduleTask, NativeScheduleUpdate, ScheduleUpdateResult, ScheduleDeleteResult } from '@hanger-source/dsh-reactive-dsh/schedule';
import type { NativeTeam, NativeSessionTeam, NativeTeamTask, TeamTaskView, TeamMemberView,
  CreateTeamTaskRequest, UpdateTeamTaskRequest } from '@hanger-source/dsh-reactive-dsh/team';
import type { NativeFiles, NativeSessionFiles, WorkspaceFileText } from '@hanger-source/dsh-reactive-dsh/files';
import type { SessionFiles, FileResource, FileWatch } from '@hanger-source/dsh-reactive/files';
import type { ResourceReadable } from '@hanger-source/dsh-reactive';
import type { NativeApproval } from '@hanger-source/dsh-reactive-dsh/approval';
import type { NativeClock } from '@hanger-source/dsh-reactive-dsh/time';
import type { NativeSkills } from '@hanger-source/dsh-reactive-dsh/skills';
import type { NativeSubagents, NativeSessionSubagents, SubagentDescendantListEntry } from '@hanger-source/dsh-reactive-dsh/subagents';

function programCapabilities<P extends ProgramContract>(conversation: ProgramSession<P>, capabilities: {
  jobs: NativeJobs; schedule: NativeSchedule; team: NativeTeam; files: NativeFiles; approval: NativeApproval; clock: NativeClock;
  skills: NativeSkills; subagents: NativeSubagents;
}) {
  const source: NativeSessionSource = conversation;
  return {
    jobs: capabilities.jobs.forSession(source), schedule: capabilities.schedule.forSession(source),
    team: capabilities.team.forSession(source), files: capabilities.files.forSession(source),
    approval: capabilities.approval.forSession(source), clock: capabilities.clock.forSession(source),
    skills: capabilities.skills.forSession(source), subagents: capabilities.subagents.forSession(source),
  };
}
void programCapabilities;

function jobConsumer(job: NativeJob, signal: AbortSignal) {
  const removed: void = job.remove({ signal });
  return { removed, state: job.state, output: job.output };
}
void jobConsumer;

function taskConsumer(task: NativeTeamTask) {
  const state: ResourceReadable<TeamTaskView | undefined> = task.state;
  const reading: Promise<TeamTaskView | undefined> = task.read();
  const updating: NativeInvocation<TeamTaskView> = task.update({ expectedRevision: 1, action: 'claim' });
  const result: Promise<TeamTaskView> = updating.result;
  // @ts-expect-error 固定任务更新只接受CAS与变更字段，调用者不能重复指定目标。
  task.update({ taskId: task.id, expectedRevision: 1, action: 'claim' });
  return { reading, updating, result, state, stream: task.state$ };
}
void taskConsumer;

function nativeInvocationConsumer<T>(call: NativeInvocation<T>, signal: AbortSignal) {
  const invocation: Invocation<T, Json, NativeInvocation<T>['source']> = call;
  const result: Promise<T> = invocation.result;
  const waiting: Promise<T> = invocation.wait({ signal });
  const activity: ResourceReadable<InvocationActivity> = call.activity;
  const kind: 'native' = call.source.kind;
  return { invocation, result, waiting, activity, kind, source: call.source, stream: call.activity$ };
}
void nativeInvocationConsumer;

function fileUploadInvocationConsumer(call: FileUploadInvocation, signal: AbortSignal) {
  const invocation: Invocation<FileUploadValue, never, FileUploadInvocation['source']> = call;
  const result: Promise<FileUploadValue> = invocation.result;
  const waiting: Promise<FileUploadValue> = invocation.wait({ signal });
  return { invocation, result, waiting, activity: invocation.activity, stream: invocation.activity$ };
}
void fileUploadInvocationConsumer;

function teamConsumer(team: NativeSessionTeam, create: CreateTeamTaskRequest, update: UpdateTeamTaskRequest) {
  const roster: ResourceReadable<readonly TeamMemberView[]> = team.roster;
  const creating: NativeInvocation<TeamTaskView> = team.createTask(create);
  const updating: NativeInvocation<TeamTaskView> = team.updateTask(update);
  return { roster, stream: team.roster$, creating, updating };
}
void teamConsumer;

function scheduleConsumer(task: NativeScheduleTask, update: NativeScheduleUpdate) {
  const updating: NativeInvocation<ScheduleUpdateResult> = task.update(update);
  const deleting: NativeInvocation<ScheduleDeleteResult> = task.delete();
  return { updating, deleting };
}
void scheduleConsumer;

function fileConsumer(files: NativeSessionFiles) {
  const shared: SessionFiles = files;
  const page: FileResource<WorkspaceFileText> = shared.text('report.txt', { offset: 2, limit: 5 });
  const watch: FileWatch = shared.watch('report.txt');
  const closing: Promise<void> = files.dispose();
  return { page, watch, closing, directory: shared.directory('.'), metadata: shared.metadata('report.txt') };
}
void fileConsumer;

function programFileConsumer<P extends ProgramContract>(conversation: ProgramSession<P>) {
  const files: SessionFiles = conversation.files;
  return { page: files.text('report.txt'), bytes: files.readBytes('report.txt', { range: { offset: 0, length: 32 } }) };
}
void programFileConsumer;

function subagentConsumer(subagents: NativeSessionSubagents, target: SubagentDescendantListEntry['id'], signal: AbortSignal) {
  const descendants: ResourceReadable<readonly SubagentDescendantListEntry[]> = subagents.descendants;
  const reading: Promise<readonly SubagentDescendantListEntry[]> = subagents.descendants.read(signal);
  const queried: Promise<readonly SubagentDescendantListEntry[]> = subagents.readDescendants({ signal });
  const interrupted: void = subagents.interrupt(target, { signal });
  return { descendants, reading, queried, interrupted, stream: subagents.descendants$ };
}
void subagentConsumer;
