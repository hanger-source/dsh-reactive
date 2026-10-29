import type { Context } from '@deepseek-ai/cordis';
import { RemoteError, type Principal } from '@deepseek-ai/dsh-typert-protocol';
import type { JsonValue } from '@deepseek-ai/dsh-util-values';
import type { ApplicationHost, RetainedActionInvocation } from '@hanger-source/dsh-reactive-dsh';
import { z, surfaceSlotDefinition, actionIdentity, assertActionIdentity, assertSharedProgramDefinition, type AnyEnvironment, type OperationHandle, type OperationContract } from '@hanger-source/dsh-reactive';
import { Inbox } from '@hanger-source/dsh-reactive-client/inbox';
import type { ProgramActionRequest, ProgramActionStart, ProgramInvocationAddress, ProgramInvocationLookupRequest, ProgramInvocationOperationRequest, ProgramInvocationFrame } from '@hanger-source/dsh-reactive-client/types';
import { createProgramFailureMapper, throwProgramFailure } from './host-errors.ts';
import { resolveProgramParticipant, type ProgramParticipant } from './participant.ts';

declare module '@deepseek-ai/dsh-typert-protocol' {
  interface RemoteErrorDetailsMap {
    'reactive/invocation-unavailable': Record<string, never>;
    'reactive/action-owner-closed': Record<string, never>;
  }
}

/** 运输宿主实际保留的Invocation；连接退出只取消观察，不撤销已经开始的业务。 */
export interface ProgramActionOwner {
  readonly signal: AbortSignal;
  start(request: ProgramActionRequest, principal: Principal, signal: AbortSignal): Promise<ProgramActionStart>;
  get(request: ProgramInvocationLookupRequest, principal: Principal, signal: AbortSignal): Promise<ProgramActionStart>;
  result(request: ProgramInvocationAddress, principal: Principal, signal: AbortSignal): Promise<JsonValue>;
  observe(request: ProgramInvocationAddress, principal: Principal, signal: AbortSignal): AsyncIterable<ProgramInvocationFrame>;
  operation(request: ProgramInvocationOperationRequest, principal: Principal, signal: AbortSignal): Promise<OperationHandle<OperationContract>>;
}

export interface ProgramActionOwnerOptions {
  /** 仅接受认证宿主签发的 Principal，由此确定业务角色和 actor。 */
  participant(principal: Principal): ProgramParticipant;
  /** owner 关闭只释放关联与观察，ApplicationHost 仍决定业务执行的生存期。 */
  readonly signal?: AbortSignal;
}

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
  const rethrow = (error: unknown): never => throwProgramFailure(failure(error));
  const lifetime = new AbortController();
  const signal = options.signal ? AbortSignal.any([options.signal, lifetime.signal]) : lifetime.signal;
  const participant = (principal: Principal): ProgramParticipant => {
    signal.throwIfAborted();
    return resolveProgramParticipant(options.participant, principal);
  };
  const lookup = (request: ProgramInvocationAddress, principal: Principal): RetainedActionInvocation => {
    const identity = participant(principal);
    try {
      return identity.participant === 'service'
        ? application.service(identity.actorId).readInvocation(request.sessionId, request.invocationId, { contract: request.program })
        : application.readInvocation(request.sessionId, request.invocationId, { contract: request.program }, { actorId: identity.actorId });
    }
    catch (error) { return rethrow(error); }
  };
  const dispose = (): void => {
    lifetime.abort(new RemoteError('reactive/action-owner-closed', 'Action owner is closed', {}));
  };
  signal.addEventListener('abort', dispose, { once: true });
  if (signal.aborted) dispose();
  return {
    signal,
    async start(request: ProgramActionRequest, principal: Principal, querySignal: AbortSignal): Promise<ProgramActionStart> {
      const identity = participant(principal);
      querySignal.throwIfAborted();
      const invocationId = z.uuid().parse(request.invocationId);
      const handle = await (identity.participant === 'service'
        ? application.service(identity.actorId).open(request.sessionId, { contract: request.program }, request.accessId)
        : application.open(request.sessionId, { contract: request.program }, request.accessId)).catch(rethrow);
      let accepted = false;
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
          identity.participant === 'human' ? { actorId: identity.actorId, invocationId } : { invocationId });
        accepted = true;
        // 回执由真实ActionAuthority保留；transport仅在真实执行结束后归还本次句柄。
        invocation.result.then(() => handle.release(), () => handle.release());
        return { id: invocation.id, name: request.name, contract: actionIdentity(definition), source: invocation.source };
      } catch (error) { return rethrow(error); }
      finally { if (!accepted) handle.release(); }
    },
    async get(request, principal, querySignal) {
      querySignal.throwIfAborted();
      const entry = lookup(request, principal);
      if (entry.name !== request.name) throw new RemoteError('reactive/invocation-unavailable', 'Invocation belongs to another Surface address', {});
      try { assertActionIdentity(request.contract, entry.definition); }
      catch (error) { return rethrow(error); }
      return { id: entry.invocation.id, name: entry.name, contract: actionIdentity(entry.definition), source: entry.invocation.source };
    },
    result(request, principal, querySignal) {
      const entry = lookup(request, principal);
      return read(entry.invocation.result, AbortSignal.any([signal, querySignal]))
        .then(value => z.json().parse(z.encode(entry.definition.output, value))).catch(rethrow);
    },
    async operation(request, principal, querySignal) {
      const entry = lookup(request, principal);
      const unavailable = () => new RemoteError('reactive/invocation-operation-unavailable', 'Invocation has no matching Operation receipt', {});
      const receipt = entry.invocation.operation;
      if (!entry.definition.operation || !receipt) throw unavailable();
      const operation = await read(receipt, AbortSignal.any([signal, querySignal])).catch(rethrow);
      if (operation.definition.kind !== request.operation.kind || operation.definition.version !== request.operation.version
        || referenceIdentity(z.json().parse(operation.reference)) !== referenceIdentity(request.reference)) throw unavailable();
      return operation;
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

/** 原句柄reference已是合同签发的wire值；只比较JSON身份，不重跑reference codec。 */
function referenceIdentity(value: JsonValue): string {
  return JSON.stringify(value, (_key, item: unknown) => item !== null && typeof item === 'object' && !Array.isArray(item)
    ? Object.fromEntries(Object.entries(item).sort(([left], [right]) => left.localeCompare(right))) : item);
}
