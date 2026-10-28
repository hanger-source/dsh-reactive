import type { Agent } from '@deepseek-ai/dsh-agent';
import type { BehaviorSubject } from 'rxjs';
import type { SessionHeader, UserMessage } from '@deepseek-ai/dsh-session';
import type { AnyEnvironment, Conversation, ProgramImplementation, ProgramInstance, Scope, Views } from '@hanger-source/dsh-reactive/kernel';
import type { ManagedWorkspace } from '../workspaces/workspaces.ts';
import type { DelegationRecord } from '../delegations.ts';
import type { NativeState } from './records.ts';
import type { ProgramSkills } from '../programs/program-skills.ts';
import type { HumanInputs } from '../participation/human-input.ts';
import type { NativeProgress } from './progress.ts';
/** 应用观察中的同一程序；冷读取不拥有原生 Agent。 */
export interface ConversationSession {
    /** 本会话事实与Program图的实际owner；冷观察保留它，收集会话时统一撤销。 */
    readonly scope: Scope;
    readonly header: SessionHeader;
    readonly workspace: ManagedWorkspace;
    readonly environment: AnyEnvironment;
    readonly conversation: Conversation<any>;
    readonly implementation: ProgramImplementation;
    readonly child?: DelegationRecord;
    readonly program: ProgramInstance<any>;
    readonly views: Views;
    state: NativeState;
    pins: number;
    /** 唯一的原生挂载引用；普通读取和长期观察共用此事实，不另存 owner 代次。 */
    readonly live: BehaviorSubject<LiveConversation | undefined>;
}
/** 一个实际原生 Agent 的挂载；队列、Goal和持久投影仍归 dsh。 */
export interface LiveConversation {
    readonly agent: Agent;
    readonly session: ConversationSession;
    /** 同一原生声明安装的观察与准入寿命，不拥有已受理业务的取消。 */
    readonly scope: Scope;
    /** 原生声明、Skill和实际安装Fiber的同一退出回执；清理失败仍会拒绝。 */
    readonly closed: Promise<void>;
    /** 撤销上述scope并等待原生声明Fiber退出；与父Agent清理共用回执。 */
    dispose(): Promise<void>;
    claimed: UserMessage[];
    stepSignal?: AbortSignal;
    preparing: boolean;
    readonly recovery: Set<Promise<void>>;
    readonly recoveryErrors: unknown[];
    readonly skills: ProgramSkills;
    readonly human: HumanInputs;
    readonly progress: NativeProgress;
    nativeMask?: () => void;
}
