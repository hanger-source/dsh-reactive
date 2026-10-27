import type { Observable } from 'rxjs';
import type { ActionImplementation, HumanInputPolicy, Observation, OperationOwner } from '@hanger-source/dsh-reactive';
import type { OnboardingTarget, MerchantTarget, PreparationFacts, KnowledgeFacts, ReceptionFacts, MerchantCommandFacts, Preparation, DeliveryCommand, KnowledgeTask, Reception, MerchantCommand, PrepareMerchant, RunPreparationCommand, ObservePreparationCommand, InitializeKnowledge, RetryKnowledge, CommitKnowledge, EnableReception, ResumeReception, ExecuteMerchantCommand, ObserveMerchantCommand, ReconcileMerchantCommand } from './contracts/index.ts';

export interface PreparationBindings {
  facts(target: OnboardingTarget): Observable<Observation<PreparationFacts>>;
  readonly actions: {
    readonly prepareMerchant: ActionImplementation<typeof PrepareMerchant>;
    readonly runPreparationCommand: ActionImplementation<typeof RunPreparationCommand>;
    readonly observePreparationCommand: ActionImplementation<typeof ObservePreparationCommand>;
  };
  readonly operations: { readonly preparation: OperationOwner<typeof Preparation>; readonly deliveryCommand: OperationOwner<typeof DeliveryCommand> };
}
export interface KnowledgeBindings {
  facts(target: OnboardingTarget): Observable<Observation<KnowledgeFacts>>;
  readonly actions: { readonly initializeKnowledge: ActionImplementation<typeof InitializeKnowledge>;
    readonly retryKnowledge: ActionImplementation<typeof RetryKnowledge>; readonly commitKnowledge: ActionImplementation<typeof CommitKnowledge> };
  readonly operations: { readonly knowledgeTask: OperationOwner<typeof KnowledgeTask> };
}
export interface ReceptionBindings {
  facts(target: OnboardingTarget): Observable<Observation<ReceptionFacts>>;
  readonly actions: { readonly enableReception: ActionImplementation<typeof EnableReception>; readonly resumeReception: ActionImplementation<typeof ResumeReception> };
  readonly operations: { readonly reception: OperationOwner<typeof Reception> };
}
export interface MerchantCommandBindings {
  facts(target: MerchantTarget): Observable<Observation<MerchantCommandFacts>>;
  readonly actions: { readonly executeMerchantCommand: ActionImplementation<typeof ExecuteMerchantCommand>;
    readonly observeMerchantCommand: ActionImplementation<typeof ObserveMerchantCommand>; readonly reconcileMerchantCommand: ActionImplementation<typeof ReconcileMerchantCommand> };
  readonly operations: { readonly merchantCommand: OperationOwner<typeof MerchantCommand> };
}
export interface MerchantBindings {
  readonly preparation: PreparationBindings;
  readonly knowledge: KnowledgeBindings;
  readonly reception: ReceptionBindings;
  readonly merchantCommands: MerchantCommandBindings;
  readonly humanInput: HumanInputPolicy;
  readonly requiredSkills: readonly string[];
}
