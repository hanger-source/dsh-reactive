import type { ProgramContract, ProgramSession } from '@hanger-source/dsh-reactive';
import type { NativeSessionSource } from '@hanger-source/dsh-reactive-dsh';
import type { NativeJobs } from '@hanger-source/dsh-reactive-dsh/jobs';
import type { NativeSchedule } from '@hanger-source/dsh-reactive-dsh/schedule';
import type { NativeTeam, NativeTeamTask, TeamTaskView } from '@hanger-source/dsh-reactive-dsh/team';
import type { NativeFiles, NativeSessionFiles, WorkspaceFileText } from '@hanger-source/dsh-reactive-dsh/files';
import type { ResourceReadable } from '@hanger-source/dsh-reactive';
import type { NativeApproval } from '@hanger-source/dsh-reactive-dsh/approval';
import type { NativeClock } from '@hanger-source/dsh-reactive-dsh/time';

function programCapabilities<P extends ProgramContract>(conversation: ProgramSession<P>, capabilities: {
  jobs: NativeJobs; schedule: NativeSchedule; team: NativeTeam; files: NativeFiles; approval: NativeApproval; clock: NativeClock;
}) {
  const source: NativeSessionSource = conversation;
  return {
    jobs: capabilities.jobs.forSession(source), schedule: capabilities.schedule.forSession(source),
    team: capabilities.team.forSession(source), files: capabilities.files.forSession(source),
    approval: capabilities.approval.forSession(source), clock: capabilities.clock.forSession(source),
  };
}
void programCapabilities;

function taskConsumer(task: NativeTeamTask) {
  const state: ResourceReadable<TeamTaskView | undefined> = task.state;
  const reading: Promise<TeamTaskView | undefined> = task.read();
  const updating: Promise<TeamTaskView> = task.update({ expectedRevision: 1, action: 'claim' });
  // @ts-expect-error 固定任务更新只接受CAS与变更字段，调用者不能重复指定目标。
  task.update({ taskId: task.id, expectedRevision: 1, action: 'claim' });
  return { reading, updating, state, stream: task.state$ };
}
void taskConsumer;

function fileConsumer(files: NativeSessionFiles) {
  const page: ResourceReadable<WorkspaceFileText> = files.text('report.txt', { offset: 2, limit: 5 });
  return { page, directory: files.directory('.'), metadata: files.metadata('report.txt') };
}
void fileConsumer;
