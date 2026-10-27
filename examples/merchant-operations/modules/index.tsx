import { Context, Expose, Module, Skill } from '@hanger-source/dsh-reactive/program';
import { type OperationFollowMode, type SurfaceMount, type ProgramSession } from '@hanger-source/dsh-reactive';
import type { MerchantBindings } from '../bindings.ts';
import { collaborationSurface } from '../module-contracts.ts';
import { merchantComposition, type MerchantProgramScope, type OperationsProgram } from '../program-contracts.ts';
import { KnowledgeModule } from './knowledge.tsx';
import { MerchantCommandsModule } from './merchant-commands.tsx';
import { PreparationModule } from './preparation.tsx';
import { ReceptionModule } from './reception.tsx';
import { useMerchantDelivery, type MerchantDelivery } from './delivery.ts';

export interface MerchantModuleTarget {
  readonly key: string;
  readonly scope: MerchantProgramScope;
  readonly mount: SurfaceMount<typeof merchantComposition.surface>;
}

/** 相同业务模块在多个目标和公开合同中复用，不需要 names 或应用自定义 Outlet。 */
export function MerchantOperationsModules({ bindings, targets, conversation, followMode = 'wait' }: {
  readonly bindings: MerchantBindings;
  readonly targets: readonly MerchantModuleTarget[];
  readonly conversation: ProgramSession<typeof OperationsProgram>;
  readonly followMode?: OperationFollowMode;
}) {
  return <>
    {[...new Set(bindings.requiredSkills)].map(name => <Skill key={name} name={name} />)}
    {targets.map(target => <Module key={target.key} mount={target.mount}>
      <MerchantModules target={target} bindings={bindings} conversation={conversation} followMode={followMode} />
    </Module>)}
  </>;
}

function MerchantModules({ target, bindings, conversation, followMode }: {
  readonly target: MerchantModuleTarget; readonly bindings: MerchantBindings;
  readonly conversation: ProgramSession<typeof OperationsProgram>; readonly followMode: OperationFollowMode;
}) {
  const work = useMerchantDelivery(bindings, target.scope, conversation, followMode);
  return <>
    <Module mount={merchantComposition.mounts.preparation}>
      <PreparationModule preparation={work.preparation} follow={work.follow} />
    </Module>
    <Module mount={merchantComposition.mounts.knowledge}>
      <KnowledgeModule knowledge={work.knowledge} follow={work.follow} />
    </Module>
    <Module mount={merchantComposition.mounts.reception}>
      <ReceptionModule reception={work.reception} follow={work.follow} />
    </Module>
    <Module mount={merchantComposition.mounts.commands}>
      <MerchantCommandsModule commands={work.commands} follow={work.follow} />
    </Module>
    <Module mount={merchantComposition.mounts.collaboration}>
      <CollaborationModule work={work} />
    </Module>
  </>;
}

function CollaborationModule({ work }: { readonly work: MerchantDelivery }) {
  const facts = work.collaboration;
  return <>
    <Context name="prerequisites" required={false} observation={facts.state} render={value => value} />
    <Context name="activity" required={false} observation={work.activity} render={value => value} />
    <Expose surface={collaborationSurface} value={{ views: { facts: facts.observations$, activity: work.activity$ } }} />
  </>;
}
