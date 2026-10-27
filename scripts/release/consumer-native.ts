import { createNativeJobs } from '@hanger-source/dsh-reactive-dsh/jobs';
import { createNativeMcp } from '@hanger-source/dsh-reactive-dsh/mcp';
import { createNativeApproval } from '@hanger-source/dsh-reactive-dsh/approval';
import { createNativeClock } from '@hanger-source/dsh-reactive-dsh/time';
import { createNativeSchedule } from '@hanger-source/dsh-reactive-dsh/schedule';
import { createNativeTeam, type NativeSessionTeam, type NativeTeamTask, type NativeTeamTaskUpdate,
  type TeamTaskId } from '@hanger-source/dsh-reactive-dsh/team';
import { createNativeFiles } from '@hanger-source/dsh-reactive-dsh/files';

// 这些是正式npm子入口；只加载与编译，不为包检查安装Runtime或执行原生副作用。
export const entries = [createNativeJobs, createNativeMcp, createNativeApproval, createNativeClock,
  createNativeSchedule, createNativeTeam, createNativeFiles] as const;
export const task = (team: NativeSessionTeam, id: TeamTaskId): Promise<NativeTeamTask> => team.task(id);
export const change = (task: NativeTeamTask, request: NativeTeamTaskUpdate) => task.update(request);
