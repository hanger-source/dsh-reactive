import { spawn } from 'node:child_process';
import { combineLatest, map, distinctUntilChanged, type Observable } from 'rxjs';
import { Context, NativeTools, useObservable } from '@hanger-source/dsh-reactive/program';
import type { AnyEnvironment, CancellationOptions, NativeToolAdmission } from '@hanger-source/dsh-reactive';
import { createRuntime, nativeCapabilities, type ApplicationFactory, type ConversationHandle, type RuntimeOptions } from '@hanger-source/dsh-reactive-dsh';
import { createNativeJobs, type NativeJobs, type JobId, type JobOutcome } from '@hanger-source/dsh-reactive-dsh/jobs';
import { createNativeMcp, type NativeMcpOptions } from '@hanger-source/dsh-reactive-dsh/mcp';
import { createNativeApproval, type NativeApprovalOptions } from '@hanger-source/dsh-reactive-dsh/approval';
import { createNativeClock } from '@hanger-source/dsh-reactive-dsh/time';

declare module '@deepseek-ai/dsh-jobs/view' {
  interface JobKindMap { applicationProcess: 'application-process'; }
}

/** 凭据/连接、实际批准回答者与业务工具政策由部署注入，不进入纯 Program 合同。 */
export interface CapabilityConfiguration {
  readonly mcp: NativeMcpOptions;
  readonly approval: NativeApprovalOptions;
  readonly businessTimeZone: string;
}

/** 一个应用可复用这组实例；不需要编写 NativeContext 插件、监听或释放顺序。 */
export function createCapabilityEnvironment(config: CapabilityConfiguration) {
  const jobs = createNativeJobs({ registry: { kind: 'local' }, tools: { completionDelivery: 'quiet' } });
  const mcp = createNativeMcp(config.mcp);
  const approval = createNativeApproval(config.approval);
  const clock = createNativeClock({ timeZone: config.businessTimeZone, refreshIntervalMs: 600_000 });
  const toolNames$ = combineLatest([jobs.toolNames$, mcp.toolNames$, mcp.resourceToolNames$]).pipe(
    map(groups => Object.freeze([...new Set(groups.flat())].sort())),
    distinctUntilChanged((left, right) => left.length === right.length && left.every((name, index) => name === right[index])),
  );
  return { jobs, mcp, approval, clock, toolNames$, install: nativeCapabilities(jobs, mcp, approval, clock) };
}

/** 独立宿主；同一 install 也可交给嵌入宿主的正式装配入口。 */
export function startCapabilityApplication<E extends readonly AnyEnvironment[]>(
  factory: ApplicationFactory<E>, options: Omit<RuntimeOptions, 'installNative'>, capabilities: ReturnType<typeof createCapabilityEnvironment>) {
  return createRuntime(factory, { ...options, installNative: capabilities.install });
}

/** 观察与模型声明是普通流组合，MCP 无工具/断开时撤声明，不生成空 NativeTools。 */
export function CapabilityModule({ capabilities, enabled$, admit }: {
  readonly capabilities: ReturnType<typeof createCapabilityEnvironment>;
  readonly enabled$: Observable<boolean>;
  readonly admit: NativeToolAdmission;
}) {
  const names = useObservable(() => capabilities.toolNames$, [capabilities]);
  const enabled = useObservable(() => enabled$, [enabled$]);
  return <>
    <Context name="business-time-zone" value={{ timeZone: capabilities.clock.timeZone }} />
    <Context name="native-tool-catalog" required={false} observation={names} render={value => value} />
    {names.status === 'ready' && names.value.length > 0 && <NativeTools names={names.value}
      enabled={enabled.status === 'ready' && enabled.value} admit={admit} />}
  </>;
}

/**
 * 一个真实 producer：已安装模块由 Node 执行，JobRegistry 持有身份、状态、输出和 owner。
 * 这里没有重建 shell 工具、重试或队列；模块自己的业务持久化与权限仍由其 owner 决定。
 */
export function startNodeModule(jobs: NativeJobs, session: ConversationHandle<any>, modulePath: string,
  args: readonly string[], options: CancellationOptions = {}) {
  const owned = jobs.forSession(session);
  const id = owned.start({ kind: 'application-process', label: `node ${modulePath}`, run(job) {
    const child = spawn(process.execPath, [modulePath, ...args], { cwd: session.identity.directory,
      env: {}, stdio: ['ignore', 'pipe', 'pipe'] });
    let cancelled = false;
    let failure: Error | undefined;
    child.stdout.setEncoding('utf8').on('data', (text: string) => job.append(text, { channel: 'stdout' }));
    child.stderr.setEncoding('utf8').on('data', (text: string) => job.append(text, { channel: 'stderr' }));
    child.once('error', error => { failure = error; });
    const done = new Promise<JobOutcome>(resolve => child.once('close', (code, signal) => resolve({
      status: failure ? 'failed' : cancelled ? 'killed' : code === 0 ? 'completed' : 'failed',
      detail: failure?.message ?? `exit=${code}, signal=${signal}`,
    })));
    return { done, cancel() {
      if (child.exitCode !== null || child.signalCode !== null || cancelled) return;
      if (!child.kill('SIGTERM')) throw new Error('APPLICATION_PROCESS_STOP_NOT_DELIVERED');
      cancelled = true;
    } };
  } }, options);
  return owned.job(id);
}

/** 只读观察的取消不停止工作，stop 与 wait 分开说明各自结果。 */
export async function observeAndStop(jobs: NativeJobs, session: ConversationHandle<any>, id: JobId,
  options: CancellationOptions = {}) {
  const observed = jobs.forSession(session);
  const job = observed.job(id);
  const output = job.readAt(0); // 可重复读取；模型 job_output 的 cursor 不受影响。
  job.stop('operator requested stop', options);
  const current = await job.wait(5_000, options); // 超时返回 stopping/running 仍不是完成。
  return { output, current };
}
