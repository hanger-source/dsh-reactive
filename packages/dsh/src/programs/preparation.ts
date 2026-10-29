import type { Context } from "@deepseek-ai/cordis";
import type { Agent } from "@deepseek-ai/dsh-agent";
import type { GoalRef } from '@deepseek-ai/dsh-goal';
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
import { observePreparation, type PreparationObserver, type ModelPreparationWait } from '@hanger-source/dsh-reactive/kernel';
import { randomUUID } from 'node:crypto';
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
        const human = entry.human.snapshot$.subscribe({
            next: value => conversation[conversationBindings]().publishHuman(value),
            error: error => conversation[conversationBindings]().publishHumanError(error),
        });
        nativeScope.own(() => human.unsubscribe());
        nativeScope.effect(() => { program.revision.get(); entry.skills.commit(program.skills); });
        program.flush();
        await entry.skills.ready(AbortSignal.any([nativeScope.signal, program.signal]));
    }
    async prepareBindings(entry: LiveConversation, nativeSignal: AbortSignal, observer?: PreparationObserver) {
        const program = entry.session.program;
        const signal = AbortSignal.any([nativeSignal, program.signal]);
        for (;;) {
            signal.throwIfAborted();
            program.flush();
            this.tools.prepareNativeTools(entry);
            const revision = program.revision.peek();
            const nativeGoal = this.ctx.goals.get(entry.agent);
            const goal: GoalRef | null = nativeGoal ? Object.freeze({ id: nativeGoal.id, revision: nativeGoal.revision }) : null;
            const goalCurrent = () => {
                const current = this.ctx.goals.get(entry.agent);
                return current ? goal?.id === current.id && goal.revision === current.revision : goal === null;
            };
            try {
                await program.prepare(signal, observer);
                const skillsStable = await entry.skills.prepare(signal, observer);
                if (!skillsStable())
                    continue;
                const contextsStable = await this.bindings.prepare(entry.agent, signal, observer);
                await program.prepare(signal, observer);
                const stable = () => {
                    if (signal.aborted)
                        return false;
                    program.flush();
                    return revision === program.revision.peek() && contextsStable() && skillsStable() && goalCurrent();
                };
                if (stable())
                    return { stable, goal: nativeGoal?.phase === 'active' ? goal : null, goalCurrent };
            }
            catch (error) {
                signal.throwIfAborted();
                program.flush();
                if (revision !== program.revision.peek() || !goalCurrent())
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
            entry.stepSignal = context.signal;
            const signal = AbortSignal.any([context.signal, entry.scope.signal, entry.session.scope.signal, entry.session.program.signal]);
            signal.throwIfAborted();
            this.sessions.assertSessionWorkspace(entry.session);
            if (entry.preparing)
                throw new Error('REACTIVE_ASSEMBLY_CONCURRENT');
            entry.preparing = true;
            entry.progress.beginPreparation();
            const id = randomUUID();
            const waiting = new Map<object, ModelPreparationWait>();
            const bindings = entry.session.conversation[conversationBindings]();
            let closed = false;
            const snapshot = () => ({ id, inputs: entry.session.state.inputs(entry.claimed), waiting: [...waiting.values()] });
            const publish = () => { if (!closed) bindings.publishPreparation(snapshot(), id); };
            const close = () => {
                if (closed) return;
                closed = true;
                waiting.clear();
                bindings.publishPreparation(null, id);
            };
            const observer: PreparationObserver = wait => {
                signal.throwIfAborted();
                const token = {};
                waiting.set(token, wait);
                publish();
                return () => { if (waiting.delete(token)) publish(); };
            };
            signal.addEventListener('abort', close, { once: true });
            let releaseInputs: (() => void) | undefined;
            try {
                bindings.publishPreparation(snapshot());
                signal.throwIfAborted();
                await observePreparation(observer, { kind: 'delegation' }, signal,
                    () => firstValueFrom(abortable(() => this.delegations.beforeStep(entry.agent, signal), signal)));
                signal.throwIfAborted();
                this.sessions.assertSessionWorkspace(entry.session);
                const inputPreparation = await observePreparation(observer, { kind: 'inputs' }, signal, async () => {
                    const prepared = await this.inputs.prepare(entry, signal);
                    releaseInputs = prepared.release;
                    return prepared;
                });
                signal.throwIfAborted();
                publish();
                // 只使用公开装配 API：重新收集已更新的声明和上下文，不修改 loop 保存的旧快照。
                for (;;) {
                    const prepared = await this.prepareBindings(entry, signal, observer);
                    const assembled = await observePreparation(observer, { kind: 'assembly' }, signal,
                        () => firstValueFrom(abortable(() => entry.agent.ctx.systemPrompt.assemble({ ...context, reactivePrepared: true }), signal)));
                    signal.throwIfAborted();
                    this.sessions.assertSessionWorkspace(entry.session);
                    if (!prepared.stable())
                        continue;
                    const declared = entry.session.program.offers;
                    const assembly = { ...assembled, tools: assembled.tools.filter(tool => this.tools.nativeToolEnabled(entry, tool.name)
                            || declared.has(tool.name) || entry.session.program.follows.has(tool.name)) };
                    entry.progress.prepared({ signal: context.signal, inputs: entry.session.state.inputs(entry.claimed),
                        goal: inputPreparation.fixedGoal ? inputPreparation.goal : prepared.goal,
                        current: prepared.goalCurrent, ready: inputPreparation.ready, release: inputPreparation.release });
                    releaseInputs = undefined;
                    return assembly;
                }
            }
            finally {
                try { releaseInputs?.(); }
                finally {
                    signal.removeEventListener('abort', close);
                    close();
                    entry.preparing = false;
                }
            }
        });
    }
}
