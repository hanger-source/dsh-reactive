import type { Agent } from '@deepseek-ai/dsh-agent';
import type { SessionHeader, UserMessage } from '@deepseek-ai/dsh-session';
import type { AnyEnvironment, Conversation, ProgramImplementation, ProgramInstance, Views } from '@hanger-source/dsh-reactive/kernel';
import type { ManagedWorkspace } from '../workspaces/workspaces.ts';
import type { DelegationRecord } from '../delegations.ts';
import type { NativeState } from './records.ts';
import type { ProgramSkills } from '../programs/program-skills.ts';
import type { HumanInputs } from '../participation/human-input.ts';
import type { NativeProgress } from './progress.ts';
/** 应用观察中的同一程序；冷读取不拥有原生 Agent。 */
export interface ConversationSession {
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
    live?: LiveConversation;
}
/** 一个实际原生 Agent 的挂载；队列、Goal和持久投影仍归 dsh。 */
export interface LiveConversation {
    readonly agent: Agent;
    readonly session: ConversationSession;
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
