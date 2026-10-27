import { useMemo } from 'react';
import { combineResources, mapResource, switchResource } from '@hanger-source/dsh-reactive';
import { useShared } from '@hanger-source/dsh-reactive/program';
import type { MerchantBindings } from '../bindings.ts';
import type { MerchantProgramScope } from '../program-contracts.ts';
import type { PreparationFacts } from '../contracts/index.ts';
import { deriveCollaboration, deriveKnowledgePrerequisites } from '../contracts/collaboration.ts';
import { assertMerchant, assertOnboarding } from './fact-identity.ts';

/** 一项商家工作只连接一次各域现场；模块与跨域规则共用这些标准流。 */
export function useMerchantResources(bindings: MerchantBindings, target: MerchantProgramScope) {
  const onboarding = useMemo(() => ({ onboardingId: target.onboardingId }), [target.onboardingId]);
  const preparation$ = useShared(() => bindings.preparation.facts(onboarding).pipe(mapResource(value => {
    assertOnboarding(value, target);
    if (value.preparation) assertOnboarding(value.preparation, target);
    if (value.command) assertOnboarding(value.command, target);
    if (value.currentPreparation) assertOnboarding(value.currentPreparation, target);
    if (value.currentDeliveryCommand) assertOnboarding(value.currentDeliveryCommand, target);
    return value;
  })), [bindings.preparation, target.onboardingId]);
  // 准备现场变化撤下旧知识现场，并重新连接 owner 的权威读取。
  const knowledge$ = useShared(() => preparation$.pipe(
    switchResource(() => bindings.knowledge.facts(onboarding).pipe(mapResource(value => {
      assertOnboarding(value, target);
      for (const task of value.tasks) assertOnboarding(task, target);
      if (value.currentTask) assertOnboarding(value.currentTask, target);
      return value;
    })), { dependencies: preparationDependencies }),
  ), [preparation$, bindings.knowledge, target.onboardingId]);
  // 接待消费当前准备与知识；依赖失效会取消旧读取，不留下上次 canEnable。
  const reception$ = useShared(() => combineResources({ preparation: preparation$, knowledge: knowledge$ }).pipe(
    switchResource(() => bindings.reception.facts(onboarding).pipe(mapResource(value => {
      assertOnboarding(value, target);
      if (value.operation) assertOnboarding(value.operation, target);
      if (value.currentReception) assertOnboarding(value.currentReception, target);
      return value;
    })), { dependencies: ({ preparation, knowledge }) => [
      ...preparationDependencies(preparation),
      knowledge.revision, knowledge.sourceActivationId, knowledge.ready, knowledge.snapshotReasonCode,
    ] }),
  ), [preparation$, knowledge$, bindings.reception, target.onboardingId]);
  const merchant = target.merchant;
  const commands$ = useShared(() => bindings.merchantCommands.facts(merchant).pipe(mapResource(value => {
    assertMerchant(value.target, merchant);
    for (const command of value.commands) assertMerchant(command, merchant);
    if (value.currentCommand) assertMerchant(value.currentCommand, merchant);
    return value;
  })), [bindings.merchantCommands, merchant.subject.kind, merchant.subject.id, merchant.sandboxId, merchant.deliveryIdentity]);
  const collaboration$ = useShared(() => combineResources({
    preparation: preparation$, knowledge: knowledge$, reception: reception$,
  }).pipe(mapResource(deriveCollaboration)), [preparation$, knowledge$, reception$]);
  const knowledgePrerequisites$ = useShared(() => combineResources({ preparation: preparation$, knowledge: knowledge$ })
    .pipe(mapResource(deriveKnowledgePrerequisites)), [preparation$, knowledge$]);
  return useMemo(() => ({ preparation$, knowledge$, reception$, commands$, knowledgePrerequisites$, collaboration$ }),
    [preparation$, knowledge$, reception$, commands$, knowledgePrerequisites$, collaboration$]);
}

export type MerchantResources = ReturnType<typeof useMerchantResources>;

/** 准备修订与正式操作现场共同决定下游查询，不把命令或可用性变化当成新查询。 */
function preparationDependencies(value: PreparationFacts): readonly unknown[] {
  const preparation = value.preparation;
  return [value.onboardingId, value.revision, preparation?.operationId, preparation?.deliveryIdentity,
    preparation?.updatedAt, preparation?.status, preparation?.errorCode];
}
