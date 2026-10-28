import type { Context } from '@deepseek-ai/cordis';
import { RemoteError, type Principal } from '@deepseek-ai/dsh-typert-protocol';
import type { JsonValue } from '@deepseek-ai/dsh-util-values';
import type { ApplicationHost } from '@hanger-source/dsh-reactive-dsh';
import { z, surfaceSlotDefinition, actionIdentity, assertActionIdentity, programIdentity, assertProgramIdentity, assertSharedProgramDefinition, type ProgramContractIdentity, type ActionIdentity, type AnyEnvironment, type Invocation, type Json } from '@hanger-source/dsh-reactive';
import { Inbox } from '@hanger-source/dsh-reactive-client/inbox';
import type { ProgramActionRequest, ProgramActionStart, ProgramInvocationAddress, ProgramInvocationFrame } from '@hanger-source/dsh-reactive-client/types';
import { decodeProgramFailure } from '@hanger-source/dsh-reactive-client/errors';
import { createProgramFailureMapper } from './host-errors.ts';
import { resolveProgramParticipant, type ProgramParticipant } from './participant.ts';

declare module '@deepseek-ai/dsh-typert-protocol' {
  interface RemoteErrorDetailsMap {
    'reactive/invocation-unavailable': Record<string, never>;
    'reactive/action-owner-closed': Record<string, never>;
  }
}

/** 宿主持有真实 Invocation；连接退出只取消观察，不撤销已经开始的业务。 */
export interface ProgramActionOwner {
  start(request: ProgramActionRequest, principal: Principal, signal: AbortSignal): Promise<ProgramActionStart>;
  get(request: ProgramInvocationAddress, principal: Principal, signal: AbortSignal): Promise<ProgramActionStart>;
  result(request: ProgramInvocationAddress, principal: Principal, signal: AbortSignal): Promise<JsonValue>;
  observe(request: ProgramInvocationAddress, principal: Principal, signal: AbortSignal): AsyncIterable<ProgramInvocationFrame>;
}

export interface ProgramActionOwnerOptions {
  /** 仅接受认证宿主签发的 Principal，由此确定业务角色和 actor。 */
  participant(principal: Principal): ProgramParticipant;
  /** owner 关闭只释放关联与观察，ApplicationHost 仍决定业务执行的生存期。 */
  readonly signal?: AbortSignal;
  /** 已完成结果在本进程中的保留时间，默认五分钟；不是持久恢复或幂等承诺。 */
  readonly completedRetentionMs?: number;
  /** 默认保留最近 1000 个已完成结果；运行中的 Invocation 不参与淘汰。 */
  readonly maxCompleted?: number;
}

type Entry = {
  readonly sessionId: string;
  readonly program: ProgramContractIdentity;
  readonly participant: ProgramParticipant['participant'];
  readonly actorId: string;
  readonly name: string;
  readonly contract: ActionIdentity;
  readonly invocation: Invocation<unknown, Json>;
  readonly wireResult: Promise<JsonValue>;
  completed?: ReturnType<typeof setTimeout>;
};

/** 查询取消与业务取消分开；不让观察者的 abort 取消共享的 result Promise。 */
function read<T>(promise: Promise<T>, signal: AbortSignal): Promise<T> {
  signal.throwIfAborted();
  return new Promise<T>((resolve, reject) => {
    const abort = (): void => reject(signal.reason);
    signal.addEventListener('abort', abort, { once: true });
    promise.then(resolve, reject).finally(() => signal.removeEventListener('abort', abort));
  });
}

/** 通过公开 Handle.invokeAction 受理确切绑定，结果与活动始终指向同一 Invocation。 */
export function createProgramActionOwner<E extends readonly AnyEnvironment[]>(ctx: Context, application: ApplicationHost<E>,
  options: ProgramActionOwnerOptions): ProgramActionOwner & { dispose(): void } {
  const failure = createProgramFailureMapper(ctx.typertGateway.wireStream);
  const rethrow = (error: unknown): never => { throw decodeProgramFailure(failure(error)); };
  const retention = options.completedRetentionMs ?? 300_000;
  const maxCompleted = options.maxCompleted ?? 1000;
  if (!Number.isSafeInteger(retention) || retention < 1 || retention > 2_147_483_647
    || !Number.isSafeInteger(maxCompleted) || maxCompleted < 1) throw new Error('REACTIVE_ACTION_RETENTION_INVALID');
  const entries = new Map<string, Entry>();
  const completed = new Set<string>();
  const lifetime = new AbortController();
  const signal = options.signal ? AbortSignal.any([options.signal, lifetime.signal]) : lifetime.signal;
  const participant = (principal: Principal): ProgramParticipant => {
    signal.throwIfAborted();
    return resolveProgramParticipant(options.participant, principal);
  };
  const evict = (id: string): void => {
    const entry = entries.get(id);
    if (entry?.completed) clearTimeout(entry.completed);
    entries.delete(id); completed.delete(id);
  };
  const lookup = (request: ProgramInvocationAddress, principal: Principal): Entry => {
    const identity = participant(principal);
    const entry = entries.get(request.invocationId);
    if (!entry || entry.sessionId !== request.sessionId || entry.actorId !== identity.actorId || entry.participant !== identity.participant) {
      throw new RemoteError('reactive/invocation-unavailable', 'Invocation is absent, expired, or belongs to another participant', {});
    }
    try { assertProgramIdentity(request.program, entry.program); }
    catch (error) { return rethrow(error); }
    return entry;
  };
  const dispose = (): void => {
    lifetime.abort(new RemoteError('reactive/action-owner-closed', 'Action owner is closed', {}));
    for (const id of entries.keys()) evict(id);
  };
  signal.addEventListener('abort', dispose, { once: true });
  if (signal.aborted) dispose();
  return {
    async start(request: ProgramActionRequest, principal: Principal, querySignal: AbortSignal): Promise<ProgramActionStart> {
      const identity = participant(principal);
      querySignal.throwIfAborted();
      const handle = await (identity.participant === 'service'
        ? application.service(identity.actorId).open(request.sessionId, { contract: request.program })
        : application.open(request.sessionId, { contract: request.program })).catch(rethrow);
      let retained = false;
      try {
        signal.throwIfAborted(); querySignal.throwIfAborted();
        const program = handle.definition;
        assertSharedProgramDefinition(program);
        const definition = surfaceSlotDefinition(handle.surface, 'actions', request.name);
        if (!definition) throw new Error('REACTIVE_ACTION_SLOT_UNKNOWN');
        assertActionIdentity(request.contract, definition);
        // 不向 execute 传 transport signal。Host 在同一公开入口核验当前 Expose/准入。
        // 动态句柄不声称拥有某个静态 Program；唯一的合同 parse 由这个真实 Host 入口执行。
        const invocation = handle.invokeAction(request.name, request.bindingId, request.input,
          identity.participant === 'human' ? { actorId: identity.actorId } : {});
        const wireResult = invocation.result.then(value => z.json().parse(z.encode(definition.output, value)))
          .catch(rethrow);
        const entry: Entry = { sessionId: request.sessionId, program: programIdentity(program), participant: identity.participant, actorId: identity.actorId,
          name: request.name, contract: actionIdentity(definition), invocation, wireResult };
        entries.set(invocation.id, entry); retained = true;
        const settle = (): void => {
          handle.release();
          if (signal.aborted || entries.get(invocation.id) !== entry) return;
          completed.add(invocation.id);
          entry.completed = setTimeout(() => evict(invocation.id), retention);
          entry.completed.unref();
          while (completed.size > maxCompleted) {
            const oldest = completed.values().next().value;
            if (oldest !== undefined) evict(oldest);
          }
        };
        // 同时安装 reject handler；结果查询次数不决定是否处理业务失败。
        wireResult.then(settle, settle);
        return { id: invocation.id, name: request.name, contract: entry.contract, source: invocation.source };
      } catch (error) { return rethrow(error); }
      finally { if (!retained) handle.release(); }
    },
    async get(request, principal, querySignal) {
      querySignal.throwIfAborted();
      const entry = lookup(request, principal);
      return { id: entry.invocation.id, name: entry.name, contract: entry.contract, source: entry.invocation.source };
    },
    result(request, principal, querySignal) {
      return read(lookup(request, principal).wireResult, AbortSignal.any([signal, querySignal]));
    },
    async *observe(request, principal, querySignal) {
      const entry = lookup(request, principal);
      const inbox = new Inbox<ProgramInvocationFrame>(1024 * 1024);
      const observing = AbortSignal.any([signal, querySignal]);
      const abort = (): void => inbox.fail(observing.reason);
      const subscription = entry.invocation.activity$.subscribe({
        next: activity => {
          try {
            const frame: ProgramInvocationFrame = activity.state === 'failed'
              ? { ...activity, error: failure(activity.error) }
              : activity.state === 'returned' ? { state: 'returned',
                ...(activity.observation !== undefined ? { observation: z.json().parse(activity.observation) } : {}),
                ...(activity.observationError !== undefined ? { observationError: failure(activity.observationError) } : {}) } : activity;
            inbox.push(frame);
          } catch (error) { inbox.fail(error); }
        }, error: (error: unknown) => inbox.fail(error), complete: () => inbox.end(),
      });
      observing.addEventListener('abort', abort, { once: true });
      try { observing.throwIfAborted(); yield* inbox; }
      finally { observing.removeEventListener('abort', abort); subscription.unsubscribe(); inbox.end(); }
    },
    dispose,
  };
}
