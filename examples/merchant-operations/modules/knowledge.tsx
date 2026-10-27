import { Context, Expose, Follow, Offer } from '@hanger-source/dsh-reactive/program';
import { knowledgeSurface } from '../module-contracts.ts';
import type { MerchantDelivery } from './delivery.ts';

export interface KnowledgeModuleProps {
  readonly knowledge: MerchantDelivery['knowledge'];
  readonly follow: MerchantDelivery['follow'];
}

export function KnowledgeModule({ knowledge: work, follow }: KnowledgeModuleProps) {
  const { facts, prerequisites, initialize, retry, commit, task } = work;
  return <>
    <Context name="facts" required={false} observation={facts.state} render={value => value} />
    <Context name="prerequisites" required={false} observation={prerequisites.state} render={value => value} />
    <Offer action={initialize} enabled={work.canInitialize} follow={follow} />
    <Offer action={retry} enabled={work.canRetry} follow={follow} />
    <Offer action={commit} enabled={work.canCommit} />
    <Expose surface={knowledgeSurface} value={{ views: { facts: facts.observations$ }, actions: { initialize, retry, commit } }} />
    {task && <>
      <Follow name="await_task" description="沿原知识任务等待下一项业务反馈；提交知识仍须显式调用正式动作。"
        operation={task} {...follow} />
      <Expose surface={knowledgeSurface} value={{ operations: { task } }} />
    </>}
  </>;
}
