import { Context, Expose, Follow, Offer } from '@hanger-source/dsh-reactive/program';
import { receptionSurface } from '../module-contracts.ts';
import type { MerchantDelivery } from './delivery.ts';

export interface ReceptionModuleProps {
  readonly reception: MerchantDelivery['reception'];
  readonly follow: MerchantDelivery['follow'];
}

export function ReceptionModule({ reception: work, follow }: ReceptionModuleProps) {
  const { facts, collaboration, enable, resume, reception } = work;
  return <>
    <Context name="facts" required={false} observation={facts.state} render={value => value} />
    <Context name="prerequisites" required={false} observation={collaboration.state} render={value => value} />
    <Offer action={enable} enabled={work.canEnable} follow={follow} />
    <Offer action={resume} enabled={work.canResume} follow={follow} />
    <Expose surface={receptionSurface} value={{ views: { facts: facts.observations$ }, actions: { enable, resume } }} />
    {reception && <>
      <Follow name="await_reception" description="沿已受理接待操作等待业务反馈；恢复由明确的 resume 动作负责。"
        operation={reception} {...follow} />
      <Expose surface={receptionSurface} value={{ operations: { reception } }} />
    </>}
  </>;
}
