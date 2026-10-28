import { revokeConversationAccess, type ClientScope } from './scope.ts';
import { RemoteSnapshotStream, type ClientRemote } from '@deepseek-ai/dsh-api-gateway/client';
import { Observable, map } from 'rxjs';
import { z, implementOperation, attachOperation, locateOperation, bindOperation, decodeOperationFeedback, type OperationContract, type OperationOwner, type OperationReference, type OperationCommand, type OperationHandle, type OperationLocation, type OperationState, type ActionSource, type Observation, type BoundOperation, type OperationFeedbackRead, type OperationFeedbackPayload } from '@hanger-source/dsh-reactive';
import { RemoteError } from '@deepseek-ai/dsh-typert-protocol';
import type { JsonValue } from '@deepseek-ai/dsh-util-values';
import type { ProgramViewFrame, ProgramSessionAddress, ProgramInvocationAddress } from './contracts/types.ts';
import type { ProgramContractIdentity } from '@hanger-source/dsh-reactive';
import { decodeProgramFailure } from './contracts/errors.ts';

/**
 * JSON 运输值只恢复为所捕获 schema 的 input，随后仍由 core 的唯一边界验证。
 * 此处不能先 parse：state transform/codec 的领域值尚未产生，也不声称已经是 output。
 */
function wireInput<S extends z.ZodType>(_schema: S, value: JsonValue): z.input<S> { return value as z.input<S>; }

export interface RemoteOperationObservation {
  readonly remote: ClientRemote;
  readonly program: ProgramContractIdentity;
  failure(error: unknown): unknown;
  observe(name: string, open: (signal: AbortSignal) => AsyncIterable<ProgramViewFrame>, signal: AbortSignal): Observable<Observation<JsonValue>>;
}
export interface RemoteOperationQueries extends RemoteOperationObservation {
  readonly address: ProgramSessionAddress;
}

/** 只拥有远程查询及其异步清理；Operation的唯一状态仍由core持有。 */
function createOperationObservation(owner: ClientScope, program: ProgramContractIdentity, failure: (error: unknown) => unknown): RemoteOperationObservation {
  const ctx = owner.context;
  return {
    remote: ctx.remote, program, failure,
    observe: (name, open, lifetime) => new Observable(subscriber => {
      lifetime.throwIfAborted();
      owner.assertActive();
      const controller = new AbortController();
      const signal = AbortSignal.any([lifetime, controller.signal]);
      let generation = 0;
      let last: Observation<JsonValue>['status'] | undefined;
      const invalidate = () => {
        if (subscriber.closed) return;
        last = 'pending'; subscriber.next({ status: 'pending', generation: ++generation });
      };
      const accept = (frame: ProgramViewFrame) => {
        if (subscriber.closed) return;
        const observed = frame.observation;
        last = observed.status; generation = Math.max(generation + 1, observed.generation);
        const value = observed.status === 'error' ? { ...observed, generation, error: failure(decodeProgramFailure(observed.error)) }
          : { ...observed, generation };
        if (!signal.aborted && !subscriber.closed) subscriber.next(value);
      };
      const normalEnd = new RemoteError('gateway/internal', 'REACTIVE_REMOTE_OPERATION_ENDED', {});
      const connection = owner.acquire(() => {
        const stream = new RemoteSnapshotStream<Extract<ProgramViewFrame, { type: 'baseline' }>, Exclude<ProgramViewFrame, { type: 'baseline' }>>(
          ctx.remote.$stream<ProgramViewFrame>({ name, open: physical => open(AbortSignal.any([signal, physical])),
            ended: () => normalEnd, carrierFailed: invalidate }), {
            name, isSnapshot: (frame): frame is Extract<ProgramViewFrame, { type: 'baseline' }> => frame.type === 'baseline',
            replace: accept, update: accept,
            failed(error) {
              if (subscriber.closed) return;
              error = failure(error);
              // 有限查询只有实际交付ready/error后才正常结束；core继续判定业务终态。
              if (!signal.aborted && !(error === normalEnd && (last === 'ready' || last === 'error'))) subscriber.error(error);
              else subscriber.complete();
            },
          });
        const reset = ctx.on('connection/reset', () => { invalidate(); stream.restart(); });
        return { stream, reset };
      }, async ({ stream, reset }) => { reset(); await stream.dispose(); }, name);
      subscriber.add(() => {
        controller.abort(new Error('REACTIVE_REMOTE_OPERATION_OBSERVATION_RELEASED'));
        void connection.release().catch(() => {});
      });
      if (owner.active && !subscriber.closed) connection.value.stream.start();
    }),
  };
}

export function createOperationQueries(owner: ClientScope, address: ProgramSessionAddress): RemoteOperationQueries {
  return { ...createOperationObservation(owner, address.program, error => revokeConversationAccess(owner, error)), address };
}

/** 回执查询借SDK寿命；Session资格错误不能撤销外层SDK或原调用。 */
export function createInvocationOperationQueries(owner: ClientScope, program: ProgramContractIdentity): RemoteOperationObservation {
  return createOperationObservation(owner, program, error =>
    error instanceof RemoteError && error.code === 'reactive/conversation-access-revoked' ? decodeProgramFailure(error) : error);
}

/** 只读 owner：Host 负责事实和核权，core 负责 state parse、共享观察和 settled。 */
function owner<D extends OperationContract>(queries: RemoteOperationQueries, definition: D): OperationOwner<D> {
  const remote = queries.remote;
  const operation = { kind: definition.kind, version: definition.version };
  return implementOperation(definition, {
    async locate(command, access) {
      const commandWire = z.json().parse(z.encode(definition.command, command));
      const reply = await remote.reactivePrograms.locateOperation({ ...queries.address, operation, command: commandWire }, access.signal);
      if (!reply.ok) throw queries.failure(reply.error);
      const located = reply.value;
      return located.status === 'accepted' ? { status: 'accepted', reference: wireInput<D['reference']>(definition.reference, located.reference),
        state: wireInput<D['state']>(definition.state, located.state) } : located;
    },
    observe(reference, access) {
      const referenceWire = z.json().parse(z.encode(definition.reference, reference));
      return queries.observe('program-operation:' + definition.kind,
        signal => remote.reactivePrograms.observeOperation({ ...queries.address, operation, reference: referenceWire }, signal), access.signal)
        .pipe(map(observed => observed.status === 'ready'
          ? { ...observed, value: wireInput<D['state']>(definition.state, observed.value) } : observed));
    },
  });
}

export function attachRemoteOperation<D extends OperationContract>(queries: RemoteOperationQueries,
  definition: D, reference: OperationReference<D>, source: ActionSource, signal: AbortSignal): OperationHandle<D> {
  return attachOperation(owner(queries, definition), reference, { sessionId: queries.address.sessionId, source, signal });
}

export function locateRemoteOperation<D extends OperationContract>(queries: RemoteOperationQueries,
  definition: D, command: OperationCommand<D>, source: ActionSource, signal: AbortSignal): Promise<OperationLocation<OperationReference<D>, OperationState<D>>> {
  return locateOperation(owner(queries, definition), command, { sessionId: queries.address.sessionId, source, signal });
}

/** 只有原Invocation地址可读取该调用实际派生的Operation，不借Session重新取得目标。 */
export function attachInvocationRemoteOperation<D extends OperationContract>(queries: RemoteOperationObservation, address: ProgramInvocationAddress,
  definition: D, reference: OperationReference<D>, source: ActionSource, signal: AbortSignal): OperationHandle<D> {
  const receipt = implementOperation(definition, {
    locate() { throw new Error('REACTIVE_INVOCATION_OPERATION_LOCATE_UNSUPPORTED'); },
    observe(reference, access) {
      const referenceWire = z.json().parse(z.encode(definition.reference, reference));
      return queries.observe('program-invocation-operation:' + address.invocationId,
        signal => queries.remote.reactivePrograms.observeInvocationOperation({ ...address,
          operation: { kind: definition.kind, version: definition.version }, reference: referenceWire }, signal), access.signal)
        .pipe(map(observed => observed.status === 'ready'
          ? { ...observed, value: wireInput<D['state']>(definition.state, observed.value) } : observed));
    },
  });
  return attachOperation(receipt, reference, { sessionId: address.sessionId, source, signal });
}

/** 读取已登记参与的快照；不创建 link、订阅未来节点或借用当前 Goal。 */
export async function readRemoteOperationFeedback<D extends OperationContract>(queries: RemoteOperationQueries,
  definition: D, command: OperationCommand<D>, participationId: string, signal: AbortSignal):
  Promise<OperationFeedbackRead<OperationReference<D>, OperationFeedbackPayload<D>>> {
  signal.throwIfAborted();
  const reply = await queries.remote.reactivePrograms.readOperationFeedback({ ...queries.address,
    operation: { kind: definition.kind, version: definition.version }, command: z.json().parse(command), participationId }, signal);
  if (!reply.ok) throw queries.failure(reply.error);
  const value = reply.value;
  if (value.status === 'unknown' || value.status === 'rejected') return value;
  // 反馈中的正式引用仍是 schema input wire；验证可逆身份后保留 wire，不交付领域 reference。
  const reference = z.json().parse(z.encode(definition.reference, z.decode(definition.reference, value.reference))) as OperationReference<D>;
  return value.status === 'pending' ? { status: 'pending', reference }
    : { status: 'ready', reference, node: decodeOperationFeedback(definition, value.node) };
}

/** 当前公开槽只解析一次；core 绑定冻结原 command，后续只读不再查询可变槽。 */
export function bindRemoteOperation<D extends OperationContract>(queries: RemoteOperationQueries,
  definition: D, command: OperationCommand<D>, source: ActionSource, signal: AbortSignal): BoundOperation<D> {
  return bindOperation(owner(queries, definition), command, {
    access: () => ({ sessionId: queries.address.sessionId, source, signal }),
    readFeedback: (binding, participationId, access) => readRemoteOperationFeedback(queries, binding.definition,
      binding.command, participationId, access.signal),
  });
}
