import { combineLatest, map } from 'rxjs';
import type { CancellationOptions, Invocation } from '@hanger-source/dsh-reactive';
import { nativeCapabilities, type NativeSessionHandle } from '@hanger-source/dsh-reactive-dsh';
import { createNativeSchedule, type NativeScheduleOptions, type ScheduleId } from '@hanger-source/dsh-reactive-dsh/schedule';
import { createNativeTeam, type NativeTeamOptions, type TeamTaskId } from '@hanger-source/dsh-reactive-dsh/team';
import { createNativeFiles } from '@hanger-source/dsh-reactive-dsh/files';
import type { NativeJob } from '@hanger-source/dsh-reactive-dsh/jobs';

/** 部署显式提供 WorkspaceFiles 及其政策；这里不隐装 Gateway 或创建提醒工具。 */
export function createManagementCapabilities(options: {
  readonly schedule: NativeScheduleOptions;
  readonly team: NativeTeamOptions;
}) {
  const schedule = createNativeSchedule(options.schedule);
  const team = createNativeTeam(options.team);
  const files = createNativeFiles({ service: 'provided' });
  return { schedule, team, files, install: nativeCapabilities(schedule, team, files) };
}

/**
 * 普通代码、业务组件和后台关系可观察同一现场。文件变化不完成 Team task、不触发业务
 * Action，也不把提醒投递 ACK 当作业务完成；命令仍由显式业务参与发起。
 */
export function observeManagement(capabilities: ReturnType<typeof createManagementCapabilities>,
  session: NativeSessionHandle, path: string, options: CancellationOptions = {}) {
  const schedule = capabilities.schedule.forSession(session);
  const team = capabilities.team.forSession(session);
  const files = capabilities.files.forSession(session);
  const watch = files.watch(path, options);
  const current$ = combineLatest({ reminders: schedule.catalog$, tasks: team.tasks$, file: watch.changes$ }).pipe(
    map(current => ({ ...current, sourceSessionId: session.id })),
  );
  return { current$, fileClosed: watch.closed, async dispose() {
    // 只结束这次复合观察；不 dispose 可被其他消费者共用的 session 能力实例。
    await watch.dispose();
  } };
}

/** 编辑使用真正观察到的完整原生记录；发生冲突时结果保留官方 miss，不猜当前值重试。 */
export async function renameExistingReminder(capabilities: ReturnType<typeof createManagementCapabilities>,
  session: NativeSessionHandle, id: ScheduleId, title: string, options: CancellationOptions = {}) {
  const task = await capabilities.schedule.forSession(session).task(id, options);
  const expected = await task.read(options);
  if (!expected) throw new Error('REMINDER_NO_LONGER_EXISTS');
  return task.update({ expected, title }, options);
}

/** Team task revision 是任务板事实，不是业务 Operation 或审批权限。 */
export async function editReviewTask(capabilities: ReturnType<typeof createManagementCapabilities>,
  session: NativeSessionHandle, id: TeamTaskId, description: string, options: CancellationOptions = {}) {
  const team = capabilities.team.forSession(session);
  const expected = team.getTask(id);
  return team.updateTask({ taskId: id, expectedRevision: expected.revision, action: 'edit', description }, options);
}

/**
 * 调用者传入已经过业务准入的 Action 调用；Job 只是执行实例。读取文件由同一个 exact
 * 会话能力完成，三种结果分别返回，不因进程退出或文件存在就替 owner 宣布业务成功。
 */
export async function readCommandEvidence<T>(capabilities: ReturnType<typeof createManagementCapabilities>,
  session: NativeSessionHandle, invoke: () => Invocation<T>, job: NativeJob, outputPath: string,
  options: CancellationOptions = {}) {
  const invocation = invoke();
  const result = await invocation.result;
  const execution = await job.wait(5_000, options);
  const output = job.readAt(0); // 不消费模型的 job_output cursor。
  const file = await capabilities.files.forSession(session).stat(outputPath, options);
  return { invocationId: invocation.id, result, execution, output, file };
}
