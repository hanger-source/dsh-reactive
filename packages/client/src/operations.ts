import type { ClientRemote } from '@deepseek-ai/dsh-api-gateway/client';
import { Observable } from 'rxjs';
import { z, implementOperation, attachOperation, locateOperation, bindOperation, decodeOperationFeedback, type OperationContract, type OperationOwner, type OperationReference, type OperationCommand, type OperationHandle, type OperationLocation, type OperationState, type ActionSource, type Observation, type BoundOperation, type OperationFeedbackRead, type OperationFeedbackPayload } from '@hanger-source/dsh-reactive';
import { RemoteError, type RemoteResult } from '@deepseek-ai/dsh-typert-protocol';
import type { JsonValue } from '@deepseek-ai/dsh-util-values';
import type { ProgramViewFrame } from './types.ts';
import { decodeProgramFailure } from './errors.ts';

function result<T>(value: RemoteResult<T>): T { if (!value.ok) throw value.error; return value.value; }

/**
 * JSON 运输值只恢复为所捕获 schema 的 input，随后仍由 core 的唯一边界验证。
 * 此处不能先 parse：state transform/codec 的领域值尚未产生，也不声称已经是 output。
 */
function wireInput<S extends z.ZodType>(_schema: S, value: JsonValue): z.input<S> { return value as z.input<S>; }

/** 只读 owner：Host 负责事实和核权，core 负责 state parse、共享观察和 settled。 */
function owner<D extends OperationContract>(remote: ClientRemote, sessionId: string, definition: D): OperationOwner<D> {
  const operation = { kind: definition.kind, version: definition.version };
  return implementOperation(definition, {
    async locate(command, access) {
      const commandWire = z.json().parse(z.encode(definition.command, command));
      const located = result(await remote.reactivePrograms.locateOperation({ sessionId, operation, command: commandWire }, access.signal));
      return located.status === 'accepted' ? { status: 'accepted', reference: wireInput<D['reference']>(definition.reference, located.reference),
        state: wireInput<D['state']>(definition.state, located.state) } : located;
    },
    observe(reference, access) {
      const referenceWire = z.json().parse(z.encode(definition.reference, reference));
      return new Observable<Observation<z.input<D['state']>>>(subscriber => {
        const controller = new AbortController();
        const signal = AbortSignal.any([access.signal, controller.signal]);
        let generation = 0;
        let last: 'empty' | 'pending' | 'ready' | 'error' = 'empty';
        // RemoteStream 保留正式 RemoteError；普通 Error 会被包装而失去本次结束身份。
        const normalEnd = new RemoteError('gateway/internal', 'REACTIVE_REMOTE_OPERATION_ENDED', {});
        void (async () => {
          try {
            for await (const item of remote.$stream<ProgramViewFrame>({
              name: 'program-operation:' + definition.kind,
              open: physical => remote.reactivePrograms.observeOperation({ sessionId, operation, reference: referenceWire }, AbortSignal.any([signal, physical])),
              ended: () => normalEnd,
              carrierFailed: () => subscriber.next({ status: 'pending', generation: ++generation }),
            })) {
              item.accept();
              const observed = item.value.observation;
              last = observed.status;
              generation = Math.max(generation + 1, observed.generation);
              subscriber.next(observed.status === 'error'
                ? { ...observed, generation, error: decodeProgramFailure(observed.error) }
                : observed.status === 'ready' ? { ...observed, generation, value: wireInput<D['state']>(definition.state, observed.value) }
                : { ...observed, generation });
            }
          } catch (error) {
            // 有限 ready/error 查询可以正常完成；空或 pending 完成仍是明确的查询错误。
            if (!signal.aborted && !(error === normalEnd && (last === 'ready' || last === 'error'))) subscriber.error(error);
          }
          finally { subscriber.complete(); }
        })();
        return () => controller.abort(new Error('REACTIVE_REMOTE_OPERATION_OBSERVATION_RELEASED'));
      });
    },
  });
}

export function attachRemoteOperation<D extends OperationContract>(remote: ClientRemote, sessionId: string,
  definition: D, reference: OperationReference<D>, source: ActionSource, signal: AbortSignal): OperationHandle<D> {
  return attachOperation(owner(remote, sessionId, definition), reference, { sessionId, source, signal });
}

export function locateRemoteOperation<D extends OperationContract>(remote: ClientRemote, sessionId: string,
  definition: D, command: OperationCommand<D>, source: ActionSource, signal: AbortSignal): Promise<OperationLocation<OperationReference<D>, OperationState<D>>> {
  return locateOperation(owner(remote, sessionId, definition), command, { sessionId, source, signal });
}

/** 读取已登记参与的快照；不创建 link、订阅未来节点或借用当前 Goal。 */
export async function readRemoteOperationFeedback<D extends OperationContract>(remote: ClientRemote, sessionId: string,
  definition: D, command: OperationCommand<D>, participationId: string, signal: AbortSignal):
  Promise<OperationFeedbackRead<OperationReference<D>, OperationFeedbackPayload<D>>> {
  signal.throwIfAborted();
  const value = result(await remote.reactivePrograms.readOperationFeedback({ sessionId,
    operation: { kind: definition.kind, version: definition.version }, command: z.json().parse(command), participationId }, signal));
  if (value.status === 'unknown' || value.status === 'rejected') return value;
  // 反馈中的正式引用仍是 schema input wire；验证可逆身份后保留 wire，不交付领域 reference。
  const reference = z.json().parse(z.encode(definition.reference, z.decode(definition.reference, value.reference))) as OperationReference<D>;
  return value.status === 'pending' ? { status: 'pending', reference }
    : { status: 'ready', reference, node: decodeOperationFeedback(definition, value.node) };
}

/** 当前公开槽只解析一次；core 绑定冻结原 command，后续只读不再查询可变槽。 */
export function bindRemoteOperation<D extends OperationContract>(remote: ClientRemote, sessionId: string,
  definition: D, command: OperationCommand<D>, source: ActionSource, signal: AbortSignal): BoundOperation<D> {
  return bindOperation(owner(remote, sessionId, definition), command, {
    access: () => ({ sessionId, source, signal }),
    readFeedback: (binding, participationId, access) => readRemoteOperationFeedback(remote, sessionId, binding.definition,
      binding.command, participationId, access.signal),
  });
}
