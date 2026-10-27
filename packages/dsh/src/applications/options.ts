import { type OperationOwnerReference } from "@hanger-source/dsh-reactive/kernel";
export interface HostOptions {
    /** 应用配置的可靠业务 owner；同 kind/version 唯一，供模块退出及进程恢复后的关联读取。 */
    operations?: readonly OperationOwnerReference[];
    model?: {
        provider: string;
        model: string;
    };
    /** 专用宿主的固定操作者；多用户入口在 submit options 中提供认证所得的身份。 */
    actorId?: string;
    /** 组件内显式 Action 调用的服务身份；不借用人的身份或当前 Goal。 */
    serviceActorId?: string;
}
