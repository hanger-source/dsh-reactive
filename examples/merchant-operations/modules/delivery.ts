import { useMemo } from 'react';
import { combineLatest, distinctUntilChanged, map, type Observable } from 'rxjs';
import { decodeProgramInputs, type InputReference, type Invocation, type Observation, type OperationFollowMode, type ProgramSession } from '@hanger-source/dsh-reactive';
import { useAction, useObservable, useOperation, useResource, useShared } from '@hanger-source/dsh-reactive/program';
import type { MerchantBindings, PreparationBindings, KnowledgeBindings, ReceptionBindings, MerchantCommandBindings } from '../bindings.ts';
import type { PreparationFacts, KnowledgeFacts, KnowledgePrerequisites, ReceptionFacts, MerchantCommandFacts, CollaborationFacts, DeliveryActivity, OnboardingTarget, MerchantTarget } from '../contracts/index.ts';
import { OperationsProgram, type MerchantProgramScope } from '../program-contracts.ts';
import { useMerchantResources } from './resources.ts';

/** 这里只统计本次函数尚未结算的调用；长期操作继续读取各域事实与 Operation。 */
function currentCalls(sources: readonly Observable<readonly Invocation<unknown>[]>[]) {
  return combineLatest(sources).pipe(map(groups => groups.reduce((total, calls) => total + calls.length, 0)), distinctUntilChanged());
}

// 分域 hook 消费下方 useMerchantDelivery 唯一装配并核过目标的资源图。
function usePreparation(target: OnboardingTarget, bindings: PreparationBindings,
  facts$: Observable<Observation<PreparationFacts>>) {
  const facts = useResource(() => facts$, [facts$]);
  const prepare = useAction(bindings.actions.prepareMerchant, () => target, [target.onboardingId]);
  const runCommand = useAction(bindings.actions.runPreparationCommand, () => target, [target.onboardingId]);
  const observeCommand = useAction(bindings.actions.observePreparationCommand, () => target, [target.onboardingId]);
  const current = facts.state.status === 'ready' ? facts.state.value : undefined;
  const original = current?.currentPreparation, delivery = current?.currentDeliveryCommand;
  const preparation = useOperation(bindings.operations.preparation, () => original ?? null,
    [original?.onboardingId, original?.sessionId, original?.invocationId]);
  const command = useOperation(bindings.operations.deliveryCommand, () => delivery ?? null,
    [delivery?.onboardingId, delivery?.operationId, delivery?.sessionId, delivery?.invocationId]);
  const active$ = useShared(() => currentCalls([prepare.activeInvocations$, runCommand.activeInvocations$]), [prepare, runCommand]);
  return { facts, prepare, runCommand, observeCommand, preparation, command, active$ };
}

function useKnowledge(target: OnboardingTarget, bindings: KnowledgeBindings,
  facts$: Observable<Observation<KnowledgeFacts>>, prerequisites$: Observable<Observation<KnowledgePrerequisites>>,
  preparation: ReturnType<typeof usePreparation>) {
  const facts = useResource(() => facts$, [facts$]);
  const prerequisites = useResource(() => prerequisites$, [prerequisites$]);
  const initialize = useAction(bindings.actions.initializeKnowledge, () => target, [target.onboardingId]);
  const retry = useAction(bindings.actions.retryKnowledge, () => target, [target.onboardingId]);
  const commit = useAction(bindings.actions.commitKnowledge, () => target, [target.onboardingId]);
  const current = facts.state.status === 'ready' ? facts.state.value : undefined;
  const original = current?.currentTask;
  const task = useOperation(bindings.operations.knowledgeTask, () => original ?? null,
    [original?.onboardingId, original?.sessionId, original?.invocationId]);
  const active$ = useShared(() => currentCalls([initialize.activeInvocations$, retry.activeInvocations$, commit.activeInvocations$]),
    [initialize, retry, commit]);
  const preparing = useObservable(() => preparation.active$, [preparation.active$]);
  const allowed = prerequisites.state.status === 'ready' ? prerequisites.state.value : undefined;
  // 准备正在改写资料时不能固定旧知识。这里不以自身调用 busy 撤销本次 Action 的准入。
  const sourceStable = preparing.status === 'ready' && preparing.value === 0;
  return { facts, prerequisites, initialize, retry, commit, task, active$,
    canInitialize: sourceStable && allowed?.canInitializeKnowledge === true,
    canRetry: sourceStable && allowed?.canRetryKnowledge === true,
    canCommit: sourceStable && allowed?.canCommitKnowledge === true };
}

function useReception(target: OnboardingTarget, bindings: ReceptionBindings,
  facts$: Observable<Observation<ReceptionFacts>>, collaboration$: Observable<Observation<CollaborationFacts>>,
  preparation: ReturnType<typeof usePreparation>, knowledge: ReturnType<typeof useKnowledge>) {
  const facts = useResource(() => facts$, [facts$]);
  const collaboration = useResource(() => collaboration$, [collaboration$]);
  const enable = useAction(bindings.actions.enableReception, () => target, [target.onboardingId]);
  const resume = useAction(bindings.actions.resumeReception, () => target, [target.onboardingId]);
  const current = facts.state.status === 'ready' ? facts.state.value : undefined;
  const original = current?.currentReception;
  const reception = useOperation(bindings.operations.reception, () => original ?? null,
    [original?.onboardingId, original?.sessionId, original?.invocationId, original?.operationId, original?.expectedUpdatedAt]);
  const active$ = useShared(() => currentCalls([enable.activeInvocations$, resume.activeInvocations$]), [enable, resume]);
  const sourceCalls$ = useShared(() => combineLatest([preparation.active$, knowledge.active$]).pipe(
    map(([preparing, indexing]) => preparing + indexing), distinctUntilChanged()), [preparation.active$, knowledge.active$]);
  const sourceCalls = useObservable(() => sourceCalls$, [sourceCalls$]);
  const allowed = collaboration.state.status === 'ready' ? collaboration.state.value : undefined;
  const sourceStable = sourceCalls.status === 'ready' && sourceCalls.value === 0;
  return { facts, collaboration, enable, resume, reception, active$,
    canEnable: sourceStable && allowed?.canEnableReception === true, canResume: current?.canResume === true };
}

function useMerchantCommands(target: MerchantTarget, bindings: MerchantCommandBindings,
  facts$: Observable<Observation<MerchantCommandFacts>>, reception: ReturnType<typeof useReception>) {
  const identity = [target.subject.kind, target.subject.id, target.sandboxId, target.deliveryIdentity];
  const facts = useResource(() => facts$, [facts$]);
  const execute = useAction(bindings.actions.executeMerchantCommand, () => target, identity);
  const observe = useAction(bindings.actions.observeMerchantCommand, () => target, identity);
  const reconcile = useAction(bindings.actions.reconcileMerchantCommand, () => target, identity);
  const current = facts.state.status === 'ready' ? facts.state.value : undefined;
  const original = current?.currentCommand;
  const command = useOperation(bindings.operations.merchantCommand, () => original ?? null,
    [original?.subject.kind, original?.subject.id, original?.sandboxId, original?.deliveryIdentity, original?.sessionId, original?.invocationId]);
  const active$ = useShared(() => currentCalls([execute.activeInvocations$]), [execute]);
  const configuring = useObservable(() => reception.active$, [reception.active$]);
  return { facts, execute, observe, reconcile, command, active$,
    canExecute: configuring.status === 'ready' && configuring.value === 0 && current?.canExecute === true,
    canObserve: current?.canObserve === true, canReconcile: current?.canReconcile === true };
}

/** 自定义业务 hook 逐层消费资源、原绑定及标准流；不拥有第二份调用或任务状态。 */
export function useMerchantDelivery(bindings: MerchantBindings, target: MerchantProgramScope,
  conversation: ProgramSession<typeof OperationsProgram>, followMode: OperationFollowMode) {
  const resources = useMerchantResources(bindings, target);
  const onboarding = useMemo(() => ({ onboardingId: target.onboardingId }), [target.onboardingId]);
  const preparation = usePreparation(onboarding, bindings.preparation, resources.preparation$);
  const knowledge = useKnowledge(onboarding, bindings.knowledge, resources.knowledge$, resources.knowledgePrerequisites$, preparation);
  const reception = useReception(onboarding, bindings.reception, resources.reception$, resources.collaboration$, preparation, knowledge);
  const commands = useMerchantCommands(target.merchant, bindings.merchantCommands, resources.commands$, reception);
  const collaboration = useResource(() => resources.collaboration$, [resources.collaboration$]);
  const activity$ = useShared(() => combineLatest({
    preparation: preparation.facts.observations$, knowledge: knowledge.facts.observations$,
    reception: reception.facts.observations$, commands: commands.facts.observations$,
    preparing: preparation.active$, indexing: knowledge.active$, configuring: reception.active$, executing: commands.active$,
    human: conversation.questions$, foreground: conversation.foregroundQuestions$, activity: conversation.activity$,
  }).pipe(map((value): DeliveryActivity => {
    const belongs = (inputs: readonly InputReference[]) => decodeProgramInputs(OperationsProgram, conversation.identity.sessionId,
      inputs.filter(input => input.sourceSessionId === conversation.identity.sessionId)).some(input => {
      const scope = input.scope;
      return scope.onboardingId === target.onboardingId && scope.merchant.subject.kind === target.merchant.subject.kind
        && scope.merchant.subject.id === target.merchant.subject.id && scope.merchant.sandboxId === target.merchant.sandboxId
        && scope.merchant.deliveryIdentity === target.merchant.deliveryIdentity;
    });
    return {
      onboardingId: target.onboardingId,
      invocations: { preparation: value.preparing, knowledge: value.indexing, reception: value.configuring, commands: value.executing },
      sources: { preparation: value.preparation.status, knowledge: value.knowledge.status, reception: value.reception.status, commands: value.commands.status },
      // null 与 sources 配对：未取得可用现场时不猜长期操作阶段，也不借上次 ready。
      operations: {
        preparation: value.preparation.status === 'ready' ? value.preparation.value.preparation?.status ?? null : null,
        knowledge: value.knowledge.status === 'ready' ? value.knowledge.value.tasks.map(task => ({ submissionId: task.submissionId, status: task.taskStatus })) : null,
        reception: value.reception.status === 'ready' ? value.reception.value.operation?.status ?? null : null,
        commands: value.commands.status === 'ready' ? value.commands.value.commands.map(command => ({ commandId: command.commandId, state: command.state })) : null,
      },
      foregroundQuestionIds: value.foreground.filter(question => belongs(question.origin.inputs)).map(question => question.callId),
      pendingQuestionIds: value.human.active.filter(question => belongs(question.origin.inputs)).map(question => question.callId),
      waitIds: value.activity.waits.filter(wait => belongs(wait.continuation.inputs)).map(wait => wait.wait.id),
      conversation: { sessionId: conversation.identity.sessionId, execution: value.activity.execution,
        goal: value.activity.goal && { id: value.activity.goal.id, revision: value.activity.goal.revision,
          phase: value.activity.goal.phase, activation: value.activity.goal.activation } },
    };
  })), [preparation.facts.observations$, knowledge.facts.observations$, reception.facts.observations$, commands.facts.observations$,
    preparation.active$, knowledge.active$, reception.active$, commands.active$, conversation, target]);
  const activity = useObservable(() => activity$, [activity$]);
  const follow = useMemo(() => ({ when: 'feedback' as const, scope: target, mode: followMode }), [target, followMode]);
  return { preparation, knowledge, reception, commands, collaboration, activity, activity$, follow };
}

export type MerchantDelivery = ReturnType<typeof useMerchantDelivery>;
