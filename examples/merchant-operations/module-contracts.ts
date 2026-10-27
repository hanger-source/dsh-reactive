import { defineSurface, resourceView, view } from '@hanger-source/dsh-reactive';
import { CommitKnowledge, DeliveryCommand, EnableReception, ExecuteMerchantCommand, InitializeKnowledge, KnowledgeTask, MerchantCommand, ObserveMerchantCommand, ObservePreparationCommand, Preparation, PrepareMerchant, Reception, ReconcileMerchantCommand, ResumeReception, RetryKnowledge, RunPreparationCommand, knowledgeFactsSchema, merchantCommandFactsSchema, preparationFactsSchema, receptionFactsSchema } from './contracts/index.ts';
import { collaborationFactsSchema, deliveryActivitySchema } from './contracts/collaboration.ts';

/** 领域模块只定义自己的局部合同；实例路径和根出口由框架组合。 */
export const preparationSurface = defineSurface({
  views: { facts: resourceView(preparationFactsSchema) },
  actions: { prepare: PrepareMerchant, runCommand: RunPreparationCommand, observeCommand: ObservePreparationCommand },
  operations: { preparation: Preparation, command: DeliveryCommand },
});
export const knowledgeSurface = defineSurface({
  views: { facts: resourceView(knowledgeFactsSchema) },
  actions: { initialize: InitializeKnowledge, retry: RetryKnowledge, commit: CommitKnowledge },
  operations: { task: KnowledgeTask },
});
export const receptionSurface = defineSurface({
  views: { facts: resourceView(receptionFactsSchema) },
  actions: { enable: EnableReception, resume: ResumeReception },
  operations: { reception: Reception },
});
export const commandSurface = defineSurface({
  views: { facts: resourceView(merchantCommandFactsSchema) },
  actions: { execute: ExecuteMerchantCommand, observe: ObserveMerchantCommand, reconcile: ReconcileMerchantCommand },
  operations: { command: MerchantCommand },
});
export const collaborationSurface = defineSurface({
  views: { facts: resourceView(collaborationFactsSchema), activity: view(deliveryActivitySchema) }, actions: {},
});
