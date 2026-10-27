import { Context, Expose, Follow, Offer } from '@hanger-source/dsh-reactive/program';
import { commandSurface } from '../module-contracts.ts';
import type { MerchantDelivery } from './delivery.ts';

export interface MerchantCommandModuleProps {
  readonly commands: MerchantDelivery['commands'];
  readonly follow: MerchantDelivery['follow'];
}

export function MerchantCommandsModule({ commands: work, follow }: MerchantCommandModuleProps) {
  const { facts, execute, observe, reconcile, command } = work;
  return <>
    <Context name="facts" required={false} observation={facts.state} render={value => value} />
    <Offer action={execute} enabled={work.canExecute} follow={follow} />
    <Offer action={observe} enabled={work.canObserve} />
    <Offer action={reconcile} enabled={work.canReconcile} />
    <Expose surface={commandSurface} value={{ views: { facts: facts.observations$ }, actions: { execute, observe, reconcile } }} />
    {command && <>
      <Follow name="await_command" description="等待明确商家、sandbox 和交付身份下这条原命令的下一项反馈，不重发命令。"
        operation={command} {...follow} />
      <Expose surface={commandSurface} value={{ operations: { command } }} />
    </>}
  </>;
}
