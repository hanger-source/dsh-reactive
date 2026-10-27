import { Context, Expose, Follow, Offer } from '@hanger-source/dsh-reactive/program';
import { preparationSurface } from '../module-contracts.ts';
import type { MerchantDelivery } from './delivery.ts';

export interface PreparationModuleProps {
  readonly preparation: MerchantDelivery['preparation'];
  readonly follow: MerchantDelivery['follow'];
}

/** 准备聚合与单条交付命令各自保留业务 owner 的原命令及完成条件。 */
export function PreparationModule({ preparation: work, follow }: PreparationModuleProps) {
  const { facts, prepare, runCommand, observeCommand, preparation, command } = work;
  const current = facts.state.status === 'ready' ? facts.state.value : undefined;
  return <>
    <Context name="facts" required={false} observation={facts.state} render={value => value} />
    <Offer action={prepare} enabled={current?.canPrepare === true} follow={follow} />
    <Offer action={runCommand} enabled={current?.canRunCommand === true} follow={follow} />
    <Offer action={observeCommand} enabled={current?.canObserveCommand === true} />
    <Expose surface={preparationSurface} value={{ views: { facts: facts.observations$ }, actions: { prepare, runCommand, observeCommand } }} />
    {preparation && <>
      <Follow name="await_preparation" description="沿原准备聚合等待下一项正式业务反馈；after 使用已处理节点的 id。"
        operation={preparation} {...follow} />
      <Expose surface={preparationSurface} value={{ operations: { preparation } }} />
    </>}
    {command && <>
      <Follow name="await_command" description="等待这条已受理交付命令的下一项权威反馈，不重新发出命令。"
        operation={command} {...follow} />
      <Expose surface={preparationSurface} value={{ operations: { command } }} />
    </>}
  </>;
}
