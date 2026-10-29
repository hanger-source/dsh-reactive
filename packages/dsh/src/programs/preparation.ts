import type { Context } from "@deepseek-ai/cordis";
import type { Agent } from "@deepseek-ai/dsh-agent";
import { type Conversation, conversationBindings } from "@hanger-source/dsh-reactive/kernel";
import { ProgramInstance } from "@hanger-source/dsh-reactive/kernel";
import { ProgramSkills } from "./program-skills.ts";
import { HumanInputs } from "../participation/human-input.ts";
import { AgentBindings } from "./bindings.ts";
import { type ManagedWorkspace } from "../workspaces/workspaces.ts";
import { NativeProgress } from "../conversations/progress.ts";
import type { ProgramImplementation, Scope } from "@hanger-source/dsh-reactive/kernel";
import { type NativeDelegations } from "../delegations.ts";
import type { ConversationSession, LiveConversation } from '../conversations/session.ts';
import type { ConversationSessions } from '../conversations/sessions.ts';
import type { ConversationInputs } from '../conversations/inputs.ts';
import type { ActionAuthority } from '../execution/authority.ts';
import type { OperationParticipation } from '../operations/participation.ts';
import type { ProgramTools } from '../execution/tools.ts';
import { firstValueFrom } from 'rxjs';
import { abortable } from '@hanger-source/dsh-reactive';
declare module '@deepseek-ai/dsh-system-prompt' {
    interface AssembleContext {
        reactivePrepared?: true;
    }
}
/** Program声明与原生Agent的装配，持有Context/Skill/工具连接而不拥有模型循环。 */
export class ConversationPrograms {
    private readonly bindings: AgentBindings;
    constructor(private readonly ctx: Context, private readonly sessions: ConversationSessions, private readonly inputs: ConversationInputs, private readonly authority: ActionAuthority, private readonly operations: OperationParticipation, private readonly tools: ProgramTools, private readonly delegations: NativeDelegations) {
        this.bindings = new AgentBindings();
    }
    release(agent: Agent): Promise<void> { return this.bindings.release(agent); }
    create(scope: Scope, conversation: Conversation<any>, implementation: ProgramImplementation, workspace: ManagedWorkspace, session: () => ConversationSession): ProgramInstance<any> {
        return new ProgramInstance(scope, implementation, conversation, workspace.context, { execution: () => this.authority.programExecution(session(), 'program'),
            admit: (action, input, call, execution, signal) => this.authority.admitAction(session(), action, input, call, execution, signal),
            operation: (action, event, execution) => this.operations.operationEvent(action, event, execution),
            operationAccess: binding => this.operations.programAccess(session(), binding),
            readOperationFeedback: (binding, id, access) => this.operations.readBoundOperationFeedback(session(), binding, id, access),
            validateFollow: follow => this.operations.validateFollow(follow, implementation),
            validateOffer: offer => this.operations.validateOffer(offer, implementation) });
    }
    createLive(agent: Agent, session: ConversationSession, previous?: LiveConversation): LiveConversation {
        let skills: ProgramSkills | undefined;
        const installation = this.bindings.create(agent, session.scope, session.program.signal, () => skills?.dispose());
        session.program.signal.throwIfAborted();
        installation.scope.assertActive();
        skills = new ProgramSkills(agent, installation.context, session.program.signal);
        return { agent, session, scope: installation.scope, closed: installation.closed, dispose: installation.dispose,
            claimed: previous ? previous.claimed : [], stepSignal: previous?.stepSignal, preparing: false,
            recovery: previous ? previous.recovery : new Set(), recoveryErrors: previous ? previous.recoveryErrors : [], skills,
            human: new HumanInputs(installation.context, agent, session.program, installation.scope), progress: previous ? previous.progress : new NativeProgress() };
    }
    async attach(entry: LiveConversation): Promise<void> {
        const { session, agent } = entry, { conversation } = session;
        const program = session.program;
        await this.bindings.ready(agent);
        this.bindings.contexts(agent, () => { program.revision.get(); return new Map(program.contexts); });
        this.bindings.tools(agent, () => {
            program.revision.get();
            return [
                ...[...program.offers.values()].filter(offer => offer.enabled.value).map(offer => this.tools.tool(entry, offer)),
                ...[...program.follows.values()].filter(follow => follow.enabled.value).map(follow => this.tools.followTool(entry, follow)),
            ];
        });
        const nativeScope = entry.scope;
        nativeScope.own(() => { entry.nativeMask?.(); entry.nativeMask = undefined; });
        const questions = entry.human.questions$.subscribe({
            next: value => conversation[conversationBindings]().publishQuestions(value),
            error: error => conversation[conversationBindings]().publishQuestionError(error),
        });
        const foreground = entry.human.foreground$.subscribe({ next: value => conversation[conversationBindings]().publishForegroundQuestions(value), error: error => program.fail(error) });
        nativeScope.own(() => { questions.unsubscribe(); foreground.unsubscribe(); });
        nativeScope.effect(() => { program.revision.get(); entry.skills.commit(program.skills); });
        program.flush();
        await entry.skills.ready(AbortSignal.any([nativeScope.signal, program.signal]));
    }
    async prepareBindings(entry: LiveConversation, nativeSignal: AbortSignal) {
        const program = entry.session.program;
        const signal = AbortSignal.any([nativeSignal, program.signal]);
        for (;;) {
            signal.throwIfAborted();
            program.flush();
            this.tools.prepareNativeTools(entry);
            const revision = program.revision.peek();
            try {
                await program.prepare(signal);
                const skillsStable = await entry.skills.prepare(signal);
                if (!skillsStable())
                    continue;
                const contextsStable = await this.bindings.prepare(entry.agent, signal);
                await program.prepare(signal);
                const stable = () => {
                    if (signal.aborted)
                        return false;
                    program.flush();
                    return revision === program.revision.peek() && contextsStable() && skillsStable();
                };
                if (stable())
                    return stable;
            }
            catch (error) {
                signal.throwIfAborted();
                program.flush();
                if (revision !== program.revision.peek())
                    continue;
                throw error;
            }
        }
    }
    installAssembly(): void {
        this.ctx.on('system-prompt/assemble', async (_assembly, context, next) => {
            const entry = context.agent && this.sessions.forAgent(context.agent);
            if (!entry || context.reactivePrepared)
                return next();
            if (!context.signal)
                throw new Error('REACTIVE_ASSEMBLY_SIGNAL_REQUIRED');
            const signal = AbortSignal.any([context.signal, entry.scope.signal, entry.session.scope.signal]);
            signal.throwIfAborted();
            this.sessions.assertSessionWorkspace(entry.session);
            await firstValueFrom(abortable(() => this.delegations.beforeStep(entry.agent, signal), signal));
            signal.throwIfAborted();
            this.sessions.assertSessionWorkspace(entry.session);
            entry.stepSignal = context.signal;
            if (entry.preparing)
                throw new Error('REACTIVE_ASSEMBLY_CONCURRENT');
            entry.progress.beginPreparation();
            entry.preparing = true;
            try {
                await this.inputs.prepare(entry, signal);
                // 只使用公开装配 API：重新收集已更新的声明和上下文，不修改 loop 保存的旧快照。
                for (;;) {
                    const stable = await this.prepareBindings(entry, signal);
                    const assembled = await firstValueFrom(abortable(() => entry.agent.ctx.systemPrompt.assemble({ ...context, reactivePrepared: true }), signal));
                    signal.throwIfAborted();
                    this.sessions.assertSessionWorkspace(entry.session);
                    if (!stable())
                        continue;
                    const declared = entry.session.program.offers;
                    return { ...assembled, tools: assembled.tools.filter(tool => this.tools.nativeToolEnabled(entry, tool.name)
                            || declared.has(tool.name) || entry.session.program.follows.has(tool.name)) };
                }
            }
            finally {
                entry.preparing = false;
            }
        });
    }
}
