import type { ConversationHandle } from '@hanger-source/dsh-reactive-dsh';
import type { RemoteProgramSession } from '@hanger-source/dsh-reactive-client';
import type { ActionInput, OperationCommand } from '@hanger-source/dsh-reactive';
import { InitializeKnowledge, Preparation, type MerchantTarget, type OnboardingTarget, type ExecuteMerchantCommand } from './contracts/index.ts';
import { OperationsProgram } from './program-contracts.ts';

/** 两个 SDK 都消费同一纯 Program；网络仅使解析绑定和受理调用变成异步。 */
export type MerchantOperationsSession = ConversationHandle<typeof OperationsProgram> | RemoteProgramSession<typeof OperationsProgram>;

/** 事实只用于准备带修订的命令；真正目标由 resolver 捕获，权限与修订由业务 owner 核验。 */
export async function beginPreparation(session: MerchantOperationsSession, onboardingId: string,
  target: OnboardingTarget, deliveryIdentity: string) {
  const merchant = session.collection('merchants').instance(onboardingId);
  const facts = await merchant.readView('preparation.facts');
  if (facts.onboardingId !== target.onboardingId) throw new Error('MERCHANT_CONSUMER_TARGET_CHANGED');
  const prepare = await merchant.resolveAction('preparation.prepare', target);
  const invocation = await prepare({ expectedRevision: facts.revision, deliveryIdentity });
  const receipt = await invocation.result;
  const operation = await invocation.operation;
  return { bindingId: prepare.bindingId, invocation, receipt, operation };
}

/** 知识任务完成与固定完整 snapshot 是两个动作，不能把任务终态当成已提交知识。 */
export async function initializeKnowledge(session: MerchantOperationsSession, onboardingId: string,
  target: OnboardingTarget, request: ActionInput<typeof InitializeKnowledge>['request']) {
  const merchant = session.collection('merchants').instance(onboardingId);
  const facts = await merchant.readView('knowledge.facts');
  if (facts.onboardingId !== target.onboardingId) throw new Error('MERCHANT_CONSUMER_TARGET_CHANGED');
  const initialize = await merchant.resolveAction('knowledge.initialize', target);
  const invocation = await initialize({ expectedRevision: facts.revision, request });
  const receipt = await invocation.result;
  const operation = await invocation.operation;
  return { bindingId: initialize.bindingId, invocation, receipt, operation };
}

export async function commitCurrentKnowledge(session: MerchantOperationsSession, onboardingId: string, target: OnboardingTarget) {
  const merchant = session.collection('merchants').instance(onboardingId);
  const facts = await merchant.readView('knowledge.facts');
  if (facts.onboardingId !== target.onboardingId) throw new Error('MERCHANT_CONSUMER_TARGET_CHANGED');
  const commit = await merchant.resolveAction('knowledge.commit', target);
  const invocation = await commit({ expectedRevision: facts.revision });
  return { bindingId: commit.bindingId, invocation, receipt: await invocation.result };
}

/** 命令的业务 input 不包含 scope，捕获的绑定明确区分商家、sandbox 与交付身份。 */
export async function executeMerchantCommand(session: MerchantOperationsSession, onboardingId: string,
  target: MerchantTarget, input: ActionInput<typeof ExecuteMerchantCommand>) {
  const execute = await session.collection('merchants').instance(onboardingId).resolveAction('commands.execute', target);
  const invocation = await execute(input);
  const receipt = await invocation.result;
  const operation = await invocation.operation;
  return { bindingId: execute.bindingId, invocation, receipt, operation };
}

/** unknown 后只调用明确的观察或核对动作；这里没有自动重发原 execute。 */
export async function inspectMerchantCommand(session: MerchantOperationsSession, onboardingId: string,
  target: MerchantTarget, commandId: string, mode: 'observe' | 'reconcile') {
  const name = mode === 'observe' ? 'commands.observe' : 'commands.reconcile';
  const inspect = await session.collection('merchants').instance(onboardingId).resolveAction(name, target);
  const invocation = await inspect({ commandId });
  return { bindingId: inspect.bindingId, invocation, state: await invocation.result };
}

/** 使用业务保存的原 command 与框架已登记的参与身份，读取本次选定的稳定节点。 */
export async function readPreparationCheckpoint(session: MerchantOperationsSession,
  command: OperationCommand<typeof Preparation>, participationId: string) {
  return session.readOperationFeedback(Preparation, command, participationId);
}
