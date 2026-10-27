import { implementAction, implementOperation, type ActionCallContext, type ActionContract, type ActionParsedInput, type ActionScope, type HumanInputPolicy, type OperationContract, type OperationOwner, type OperationOwnerOptions, type OperationOwnerReference, type z } from '@hanger-source/dsh-reactive';
import { Preparation, DeliveryCommand, KnowledgeTask, Reception, MerchantCommand, PrepareMerchant, RunPreparationCommand, ObservePreparationCommand, InitializeKnowledge, RetryKnowledge, CommitKnowledge, EnableReception, ResumeReception, ExecuteMerchantCommand, ObserveMerchantCommand, ReconcileMerchantCommand } from './contracts/index.ts';
import type { MerchantBindings, PreparationBindings, KnowledgeBindings, ReceptionBindings, MerchantCommandBindings } from './bindings.ts';

/** 业务使用真实调用来源和原输入，不接管 Goal、wait 或 inbox。 */
export type BusinessAccess = Pick<ActionCallContext<unknown>, 'sessionId' | 'invocationId' | 'source' | 'inputs' | 'signal' | 'askHuman'>;
export type BusinessMethod<D extends ActionContract> = (target: ActionScope<D>, input: ActionParsedInput<D>, access: BusinessAccess) =>
  z.input<D['output']> | PromiseLike<z.input<D['output']>>;

/** 所有函数都来自真实业务集成；工厂不提供内存后端或默认成功实现。 */
export interface MerchantServices {
  readonly preparation: {
    readonly facts: PreparationBindings['facts'];
    readonly prepare: BusinessMethod<typeof PrepareMerchant>;
    readonly runCommand: BusinessMethod<typeof RunPreparationCommand>;
    readonly observeCommand: BusinessMethod<typeof ObservePreparationCommand>;
  };
  readonly knowledge: {
    readonly facts: KnowledgeBindings['facts'];
    readonly initialize: BusinessMethod<typeof InitializeKnowledge>;
    readonly retry: BusinessMethod<typeof RetryKnowledge>;
    readonly commit: BusinessMethod<typeof CommitKnowledge>;
  };
  readonly reception: {
    readonly facts: ReceptionBindings['facts'];
    readonly enable: BusinessMethod<typeof EnableReception>;
    readonly resume: BusinessMethod<typeof ResumeReception>;
  };
  readonly merchantCommands: {
    readonly facts: MerchantCommandBindings['facts'];
    readonly execute: BusinessMethod<typeof ExecuteMerchantCommand>;
    readonly observe: BusinessMethod<typeof ObserveMerchantCommand>;
    readonly reconcile: BusinessMethod<typeof ReconcileMerchantCommand>;
  };
}
export interface MerchantOwners {
  readonly preparation: OperationOwner<typeof Preparation>;
  readonly deliveryCommand: OperationOwner<typeof DeliveryCommand>;
  readonly knowledgeTask: OperationOwner<typeof KnowledgeTask>;
  readonly reception: OperationOwner<typeof Reception>;
  readonly merchantCommand: OperationOwner<typeof MerchantCommand>;
}
/** 多阶段可靠参与必须有真实业务登记和固定节点读取，不能以普通订阅替代。 */
export type ReliableOwnerOptions<D extends OperationContract> = OperationOwnerOptions<D> & {
  readonly registerFollow: NonNullable<OperationOwnerOptions<D>['registerFollow']>;
  readonly readFeedback: NonNullable<OperationOwnerOptions<D>['readFeedback']>;
};
export interface MerchantOwnerServices {
  readonly preparation: ReliableOwnerOptions<typeof Preparation>;
  readonly deliveryCommand: ReliableOwnerOptions<typeof DeliveryCommand>;
  readonly knowledgeTask: ReliableOwnerOptions<typeof KnowledgeTask>;
  readonly reception: ReliableOwnerOptions<typeof Reception>;
  readonly merchantCommand: ReliableOwnerOptions<typeof MerchantCommand>;
}

/** 直接复用 core 的事实、反馈解析与查询共享；调用方提供真实领域实现。 */
export function createMerchantOwners(services: MerchantOwnerServices): MerchantOwners {
  return Object.freeze({
    preparation: implementOperation(Preparation, services.preparation),
    deliveryCommand: implementOperation(DeliveryCommand, services.deliveryCommand),
    knowledgeTask: implementOperation(KnowledgeTask, services.knowledgeTask),
    reception: implementOperation(Reception, services.reception),
    merchantCommand: implementOperation(MerchantCommand, services.merchantCommand),
  });
}

function access(call: ActionCallContext<unknown>): BusinessAccess {
  return Object.freeze({ sessionId: call.sessionId, invocationId: call.invocationId,
    source: call.source, inputs: call.inputs, signal: call.signal, askHuman: call.askHuman });
}

/** 只做机械装配。业务权限、幂等受理、修订与真实完成仍由 services/owners 核验。 */
export function createMerchantBindings(services: MerchantServices, owners: MerchantOwners,
  options: { readonly humanInput: HumanInputPolicy; readonly requiredSkills: readonly string[] }): MerchantBindings {
  const bindings: MerchantBindings = {
    preparation: {
      facts: target => services.preparation.facts(target),
      actions: {
        prepareMerchant: implementAction(PrepareMerchant, { operationOwner: owners.preparation,
          execute: (input, call) => services.preparation.prepare(call.scope, input, access(call)) }),
        runPreparationCommand: implementAction(RunPreparationCommand, { operationOwner: owners.deliveryCommand,
          execute: (input, call) => services.preparation.runCommand(call.scope, input, access(call)) }),
        observePreparationCommand: implementAction(ObservePreparationCommand, {
          execute: (input, call) => services.preparation.observeCommand(call.scope, input, access(call)) }),
      },
      operations: { preparation: owners.preparation, deliveryCommand: owners.deliveryCommand },
    },
    knowledge: {
      facts: target => services.knowledge.facts(target),
      actions: {
        initializeKnowledge: implementAction(InitializeKnowledge, { operationOwner: owners.knowledgeTask,
          execute: (input, call) => services.knowledge.initialize(call.scope, input, access(call)) }),
        retryKnowledge: implementAction(RetryKnowledge, { operationOwner: owners.knowledgeTask,
          execute: (input, call) => services.knowledge.retry(call.scope, input, access(call)) }),
        commitKnowledge: implementAction(CommitKnowledge, {
          execute: (input, call) => services.knowledge.commit(call.scope, input, access(call)) }),
      },
      operations: { knowledgeTask: owners.knowledgeTask },
    },
    reception: {
      facts: target => services.reception.facts(target),
      actions: {
        enableReception: implementAction(EnableReception, { operationOwner: owners.reception,
          execute: (input, call) => services.reception.enable(call.scope, input, access(call)) }),
        resumeReception: implementAction(ResumeReception, { operationOwner: owners.reception,
          execute: (input, call) => services.reception.resume(call.scope, input, access(call)) }),
      },
      operations: { reception: owners.reception },
    },
    merchantCommands: {
      facts: target => services.merchantCommands.facts(target),
      actions: {
        executeMerchantCommand: implementAction(ExecuteMerchantCommand, { operationOwner: owners.merchantCommand,
          execute: (input, call) => services.merchantCommands.execute(call.scope, input, access(call)) }),
        observeMerchantCommand: implementAction(ObserveMerchantCommand, {
          execute: (input, call) => services.merchantCommands.observe(call.scope, input, access(call)) }),
        reconcileMerchantCommand: implementAction(ReconcileMerchantCommand, {
          execute: (input, call) => services.merchantCommands.reconcile(call.scope, input, access(call)) }),
      },
      operations: { merchantCommand: owners.merchantCommand },
    },
    humanInput: options.humanInput,
    requiredSkills: Object.freeze([...options.requiredSkills]),
  };
  return Object.freeze(bindings);
}

/** 交给宿主既有 operations 配置；没有第二份 owner 注册表。 */
export function merchantOperationOwners(bindings: MerchantBindings): readonly OperationOwnerReference[] {
  return [bindings.preparation.operations.preparation, bindings.preparation.operations.deliveryCommand,
    bindings.knowledge.operations.knowledgeTask, bindings.reception.operations.reception, bindings.merchantCommands.operations.merchantCommand];
}
