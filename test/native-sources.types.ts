import type { ProgramContract, ProgramSession } from '@hanger-source/dsh-reactive';
import type { NativeSessionSource } from '@hanger-source/dsh-reactive-dsh';
import type { NativeJobs } from '@hanger-source/dsh-reactive-dsh/jobs';
import type { NativeSchedule } from '@hanger-source/dsh-reactive-dsh/schedule';
import type { NativeTeam } from '@hanger-source/dsh-reactive-dsh/team';
import type { NativeFiles } from '@hanger-source/dsh-reactive-dsh/files';
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
