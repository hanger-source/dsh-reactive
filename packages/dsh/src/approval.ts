import Approval, { type ApprovalPolicy, type ApprovalOutcome, type ApprovalRequest } from '@deepseek-ai/dsh-user-approval';
import type { ApprovalRequestEvent } from '@deepseek-ai/dsh-user-approval/types';
import type { ToolExecution } from '@deepseek-ai/dsh-tools';
import type { SessionEvent } from '@deepseek-ai/dsh-session';
import { filter, map, type Observable } from 'rxjs';
import { mapResource, type ResourceReader, type CancellationOptions } from '@hanger-source/dsh-reactive';
import type { Scope } from '@hanger-source/dsh-reactive/kernel';
import { NativeCapabilityOwner, nativeObservation, nativeResource, nativeObservationScope, ownNativeValue, type NativeCapability, type NativeSessionSource } from './native-capability.ts';
import { nativeSessionAccess, nativeSessionEvents, nativeSessionInspections, type NativeSessionAccess } from './conversations/session-access.ts';

export type { ApprovalPolicy, ApprovalOutcome } from '@deepseek-ai/dsh-user-approval';
export type { ApprovalRequestEvent } from '@deepseek-ai/dsh-user-approval/types';
export type NativeApprovalEvent = SessionEvent<'approval/asked' | 'approval/decided' | 'approval/policy'>;
export interface NativeApprovalRequirement {
  readonly reason: string;
  readonly displayReason?: { readonly en: string; readonly [locale: string]: string };
}
export interface NativeApprovalOptions {
  readonly policy: ApprovalPolicy;
  /** 只在原生政策已 allow 后提出本部署的额外批准要求，不提升其他政策的拒绝。 */
  readonly require: (call: Readonly<ToolExecution>) => NativeApprovalRequirement | undefined;
  /** provided 让已安装的原生 UI 回答；没有回答者时原生服务仍 fail closed。 */
  readonly answer: 'provided' | ((request: ApprovalRequestEvent, next: () => Promise<ApprovalOutcome>) => Promise<ApprovalOutcome>);
}

export interface NativeSessionApproval {
  readonly audit$: Observable<NativeApprovalEvent>;
  /** 当前政策及读取有效性；政策是原生配置事实，不是任何一次工具调用的grant。 */
  readonly policy: ResourceReader<ApprovalPolicy>;
  readonly policy$: Observable<ApprovalPolicy>;
  readAudit(options?: CancellationOptions): Promise<readonly NativeApprovalEvent[]>;
  setPolicy(policy: ApprovalPolicy, options?: CancellationOptions): void;
}

class SessionApproval implements NativeSessionApproval {
  readonly audit$: Observable<NativeApprovalEvent>;
  readonly policy: ResourceReader<ApprovalPolicy>;
  readonly policy$: Observable<ApprovalPolicy>;
  #scope: Scope;
  constructor(private readonly owner: NativeCapabilityOwner, private readonly access: NativeSessionAccess, defaultPolicy: ApprovalPolicy) {
    this.#scope = nativeObservationScope(owner, access);
    this.audit$ = nativeObservation(this.#scope, nativeSessionEvents(access).pipe(filter((event): event is NativeApprovalEvent =>
      event.type === 'approval/asked' || event.type === 'approval/decided' || event.type === 'approval/policy'), map(ownNativeValue)));
    // 直接读取正式 policy 事件；没有事件也交付明确部署默认政策，不等下一次批准。
    this.policy = nativeResource(this.#scope, () => nativeSessionInspections(access, event => event.type === 'approval/policy').pipe(
      mapResource(inspection => {
        let current = defaultPolicy;
        for (const event of inspection.events) if (event.type === 'approval/policy') current = event.data.policy;
        return current;
      }),
    ));
    this.policy$ = this.policy.values$;
  }
  /** 完整原生日志审计；历史 asked 不是一个仍待批准的请求或可再次使用的 grant。 */
  async readAudit(options: CancellationOptions = {}): Promise<readonly NativeApprovalEvent[]> {
    this.#scope.assertActive(); this.owner.session(this.access, 'approval');
    const signal = options.signal ? AbortSignal.any([this.#scope.signal, options.signal]) : this.#scope.signal;
    signal.throwIfAborted();
    const inspection = await this.access.inspect(signal);
    signal.throwIfAborted(); this.owner.session(this.access, 'approval');
    if (inspection.meta.id !== this.access.sessionId) throw new Error('REACTIVE_NATIVE_SESSION_MISMATCH');
    return ownNativeValue(inspection.events.filter((event): event is NativeApprovalEvent =>
      event.type === 'approval/asked' || event.type === 'approval/decided' || event.type === 'approval/policy'));
  }
  setPolicy(policy: ApprovalPolicy, options: CancellationOptions = {}): void {
    this.#scope.assertActive(); options.signal?.throwIfAborted();
    const ctx = this.owner.session(this.access, 'approval');
    const agent = this.access.liveAgent();
    if (!agent || agent.id !== this.access.sessionId) throw new Error('REACTIVE_CONVERSATION_NOT_RUNNING');
    const service: Approval | undefined = ctx.get('approval');
    if (!service) throw new Error('REACTIVE_NATIVE_APPROVAL_REQUIRED');
    service.setPolicy(agent, policy);
  }
}

export interface NativeApproval extends NativeCapability {
  forSession(session: NativeSessionSource): NativeSessionApproval;
}

/** 原生 turn/one-shot grant/audit/取消保持同一个服务，业务问答不能充当批准。 */
export function createNativeApproval(options: NativeApprovalOptions): NativeApproval {
  if (options.policy !== 'ask' && options.policy !== 'never') throw new Error('REACTIVE_NATIVE_APPROVAL_POLICY_INVALID');
  if (typeof options.require !== 'function' || (options.answer !== 'provided' && typeof options.answer !== 'function')) {
    throw new Error('REACTIVE_NATIVE_APPROVAL_HANDLER_REQUIRED');
  }
  const sessions = new WeakMap<object, NativeSessionApproval>();
  const policy = options.policy, require = options.require, answer = options.answer;
  class CapabilityApproval extends Approval {
    override request(request: ApprovalRequest): Promise<ApprovalOutcome> {
      owner.scope.assertActive();
      const signal = request.signal ? AbortSignal.any([owner.scope.signal, request.signal]) : owner.scope.signal;
      // 取消进入官方request；审计、决策与晚答丢弃仍由原生服务持有。
      return super.request({ ...request, signal });
    }
  }
  const owner: NativeCapabilityOwner = new NativeCapabilityOwner('reactive-native-approval', ['approval', 'tools'], function* (ctx) {
    yield ctx.plugin(CapabilityApproval, { policy });
    if (answer !== 'provided') yield ctx.on('approval/request', async (request, next) => {
      const assertActive = () => { owner.scope.assertActive(); request.signal?.throwIfAborted(); };
      assertActive();
      const outcome = await answer(request, async () => {
        assertActive();
        const delegated = await next();
        assertActive();
        return delegated;
      });
      assertActive();
      return outcome;
    });
    yield ctx.on('tools/pre-execute', async (execution, next) => {
      owner.scope.assertActive();
      const previous = await next();
      if (previous.kind !== 'allow') return previous;
      owner.scope.assertActive();
      execution.signal.throwIfAborted();
      const requirement = require(execution);
      owner.scope.assertActive();
      execution.signal.throwIfAborted();
      return requirement === undefined ? previous : { kind: 'ask', reason: requirement.reason,
        ...(requirement.displayReason === undefined ? {} : { displayReason: requirement.displayReason }) };
    });
  });
  return Object.freeze({ install: owner.install, dispose: () => owner.dispose(),
    forSession(session: NativeSessionSource) {
      const access = nativeSessionAccess(session); owner.session(access, 'approval');
      let current = sessions.get(session);
      if (!current) {
        const reader = new SessionApproval(owner, access, policy);
        current = Object.freeze({ audit$: reader.audit$, policy: reader.policy, policy$: reader.policy$,
          readAudit: reader.readAudit.bind(reader), setPolicy: reader.setPolicy.bind(reader) });
        sessions.set(session, current);
      }
      return current;
    },
  });
}
