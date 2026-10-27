import { collectionSurface, composeSurface, defineProgram, selectSurface, z } from '@hanger-source/dsh-reactive';
import { merchantTargetSchema, onboardingTargetSchema } from './contracts/index.ts';
import { collaborationSurface, commandSurface, knowledgeSurface, preparationSurface, receptionSurface } from './module-contracts.ts';

/** 输入选择业务范围；接入单与商家运行目标的对应关系仍由宿主业务准入核验。 */
export const merchantProgramInputSchema = onboardingTargetSchema.extend({ merchant: merchantTargetSchema }).strict();
export type MerchantProgramScope = z.input<typeof merchantProgramInputSchema>;

export const merchantComposition = composeSurface({
  preparation: preparationSurface,
  knowledge: knowledgeSurface,
  reception: receptionSurface,
  commands: commandSurface,
  collaboration: collaborationSurface,
});

export const operationsComposition = composeSurface({ merchants: collectionSurface(merchantComposition.surface) });
export const operationsSurface = operationsComposition.surface;

/** 同一组领域模块也可服务只公开观察、知识固定和核对命令的应用出口。 */
export const reviewComposition = composeSurface({ merchants: collectionSurface(selectSurface(merchantComposition.surface, {
  views: ['preparation.facts', 'knowledge.facts', 'reception.facts', 'commands.facts', 'collaboration.facts', 'collaboration.activity'],
  actions: ['preparation.observeCommand', 'knowledge.commit', 'commands.observe', 'commands.reconcile'],
})) });
export const reviewSurface = reviewComposition.surface;

export const OperationsProgram = defineProgram({ input: merchantProgramInputSchema, surface: operationsSurface });
export const ReviewProgram = defineProgram({ input: merchantProgramInputSchema, surface: reviewSurface });
