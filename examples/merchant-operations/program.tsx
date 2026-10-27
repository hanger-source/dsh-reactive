import { Context, HumanInput, implementProgram, useObservable } from '@hanger-source/dsh-reactive/program';
import { type ConversationIdentity } from '@hanger-source/dsh-reactive';
import { map, type Observable } from 'rxjs';
import type { MerchantBindings } from './bindings.ts';
import { MerchantOperationsModules } from './modules/index.tsx';
import { merchantProgramInputSchema, OperationsProgram, operationsComposition, ReviewProgram, reviewComposition, type MerchantProgramScope } from './program-contracts.ts';

/** 业务 owner 提供当前会话可观察的对象集合；新增对象不重建根合同或会话。 */
export function createMerchantPrograms(bindings: MerchantBindings,
  observeTargets: (identity: ConversationIdentity) => Observable<readonly MerchantProgramScope[]>) {
  const admitResponder = (admission: Parameters<MerchantBindings['humanInput']['admitResponder']>[0]) =>
    bindings.humanInput.admitResponder(admission);
  return {
    operations: implementProgram(OperationsProgram, { Component({ conversation }) {
      const targets = useObservable(() => observeTargets(conversation.identity).pipe(map(values => values.map(value => merchantProgramInputSchema.parse(value)))), [conversation.identity]);
      return <>
        <HumanInput admitResponder={admitResponder} />
        <Context name="merchant-targets" observation={targets} render={value => value} />
        {targets.status === 'ready' && <MerchantOperationsModules bindings={bindings} conversation={conversation} targets={targets.value.map(scope => ({
          key: scope.onboardingId, scope, mount: operationsComposition.mounts.merchants.instance(scope.onboardingId),
        }))} />}
      </>;
    } }),
    review: implementProgram(ReviewProgram, { Component({ conversation }) {
      const targets = useObservable(() => observeTargets(conversation.identity).pipe(map(values => values.map(value => merchantProgramInputSchema.parse(value)))), [conversation.identity]);
      return <>
        <HumanInput admitResponder={admitResponder} />
        <Context name="merchant-targets" observation={targets} render={value => value} />
        {targets.status === 'ready' && <MerchantOperationsModules bindings={bindings} conversation={conversation} targets={targets.value.map(scope => ({
          key: scope.onboardingId, scope, mount: reviewComposition.mounts.merchants.instance(scope.onboardingId),
        }))} />}
      </>;
    } }),
  };
}
