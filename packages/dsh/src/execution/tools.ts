import type { Context } from "@deepseek-ai/cordis";
import { AsyncLocalStorage } from 'node:async_hooks';
import type { Agent } from "@deepseek-ai/dsh-agent";
import type { ToolDefinition, ToolExecution } from "@deepseek-ai/dsh-tools";
import { firstValueFrom } from "rxjs";
import { z } from "zod";
import { abortable, type Eligibility, type ResourceReader, type NativeToolAdmission } from "@hanger-source/dsh-reactive";
import { observeBoundAction, retainEligibility } from "@hanger-source/dsh-reactive/kernel";
import { type BoundOffer, type BoundFollow } from "@hanger-source/dsh-reactive/kernel";
import { toolSchema } from "./schema.ts";
import { type Workspaces } from "../workspaces/workspaces.ts";
import type { LiveConversation } from '../conversations/session.ts';
import type { ConversationSessions } from '../conversations/sessions.ts';
import type { ActionAuthority } from './authority.ts';
import type { OperationParticipation } from '../operations/participation.ts';
const nativeTools = new Set(['run_code', 'skill', 'get_goal', 'create_goal', 'update_goal']);
type NativeAdmission = { readonly entry: LiveConversation; readonly admit: NativeToolAdmission | undefined;
    readonly eligibility: ResourceReader<Eligibility> | undefined; readonly qualification: ReturnType<typeof retainEligibility> | undefined;
    readonly signal: AbortSignal; readonly stop: () => void };
type GoalDispatch = {
    readonly entry: LiveConversation;
    readonly exec: ToolExecution;
    readonly name: string;
    readonly callId: string;
    readonly rootCallId: string;
    readonly callSequence: number;
    readonly turn: number;
    readonly step: number;
    readonly signal: AbortSignal;
    active: boolean;
};
/** 原生工具与 PTC 取得同一能力表；缓存仅属于真实 Agent 和声明绑定。 */
export class ProgramTools {
    #toolDefinitions = new WeakMap<Agent, WeakMap<BoundOffer | BoundFollow, ToolDefinition>>();
    // 只证明本次原生调用已通过哪份政策；最终guard或原生结果交付即释放。
    #nativeAdmissions = new WeakMap<ToolExecution, NativeAdmission>();
    #goalDispatch = new AsyncLocalStorage<GoalDispatch>();
    constructor(private readonly ctx: Context, private readonly workspaces: Workspaces, private readonly sessions: ConversationSessions, private readonly authority: ActionAuthority, private readonly operations: OperationParticipation) { }
    private releaseNativeAdmission(exec: ToolExecution) {
        const admission = this.#nativeAdmissions.get(exec);
        this.#nativeAdmissions.delete(exec);
        try { admission?.stop(); } finally { admission?.qualification?.release(); }
        return admission;
    }
    nativeToolEnabled(entry: LiveConversation, name: string): boolean {
        const declared = entry.session.program.nativeTools[name];
        if (declared) {
            if (!declared.enabled) return false;
            const eligibility = declared.eligibility?.peek();
            return eligibility === undefined || (eligibility.status === 'ready' && eligibility.value.allowed);
        }
        return name === 'ask_user_question' ? !!entry.session.program.humanInput : nativeTools.has(name);
    }
    prepareNativeTools(entry: LiveConversation) {
        entry.nativeMask?.();
        entry.nativeMask = undefined;
        const program = entry.session.program;
        for (const tool of Object.values(program.nativeTools)) {
            if (!entry.agent.ctx.tools.get(tool.name, entry.agent))
                throw new Error('REACTIVE_NATIVE_TOOL_NOT_INSTALLED: ' + tool.name);
        }
        const deny = entry.agent.ctx.tools.schemas(entry.agent).map(tool => tool.name).filter(name => name !== 'run_code'
            && !this.nativeToolEnabled(entry, name) && !program.offers.has(name) && !program.follows.has(name));
        if (deny.length)
            entry.nativeMask = entry.agent.ctx.tools.restrict({ deny });
    }
    tool(entry: LiveConversation, offer: BoundOffer): ToolDefinition {
        let definitions = this.#toolDefinitions.get(entry.agent);
        if (!definitions) {
            definitions = new WeakMap();
            this.#toolDefinitions.set(entry.agent, definitions);
        }
        const existing = definitions.get(offer);
        if (existing)
            return existing;
        const contract = offer.action.definition;
        const allowed = () => entry.session.program.offers.get(offer.name) === offer && offer.enabled.peek();
        const definition: ToolDefinition = { name: offer.name,
            get description() {
                return contract.description + (!offer.follow ? '' : offer.follow.mode === 'background'
                    ? '\n受理后在后台观察原操作，当前工作可以继续；后续结果仍沿原工作参与。回执不表示业务已经完成。'
                    : '\n受理后暂缓当前工作的自动推进，等原操作反馈后再继续。回执不表示业务已经完成。');
            },
            isConcurrencySafe: () => contract.concurrency === 'parallel',
            parameters: toolSchema(contract.input),
            output: { schema: toolSchema(contract.output, 'output'),
                render: (_args, result) => [{ type: 'text', text: JSON.stringify(result) }],
                presentationMeta: (_args, result) => ({
                    reactiveAction: { version: 2, action: contract.name, actionVersion: contract.version, bindingId: offer.action.bindingId, modulePath: [...offer.modulePath],
                        scope: z.json().parse(offer.action.scope), output: z.json().parse(result),
                        ...contract.operation ? { operation: { kind: contract.operation.definition.kind, version: contract.operation.definition.version,
                                reference: z.json().parse(z.encode(contract.operation.definition.reference, z.decode(contract.operation.definition.reference, contract.operation.reference(result as never)))) } } : {},
                    },
                    ...contract.observe ? { reactive: z.json().parse(observeBoundAction(offer.action, result)) } : {},
                }),
            },
            execute: async (raw, exec) => {
                const program = entry.session.program;
                program.flush();
                if (!allowed())
                    throw new Error('REACTIVE_CAPABILITY_UNAVAILABLE: ' + offer.name);
                return program.invoke(offer.action, raw, this.authority.call(entry, exec, offer,
                    (continuation, wait) => this.operations.registerWait(entry.agent, continuation, wait)), allowed).result;
            },
        };
        definitions.set(offer, definition);
        return definition;
    }
    followTool(entry: LiveConversation, follow: BoundFollow): ToolDefinition {
        let definitions = this.#toolDefinitions.get(entry.agent);
        if (!definitions) {
            definitions = new WeakMap();
            this.#toolDefinitions.set(entry.agent, definitions);
        }
        const existing = definitions.get(follow);
        if (existing)
            return existing;
        const feedbackInput = z.object({ after: z.string().min(1).nullable() }).strict(), settledInput = z.object({}).strict();
        const input = follow.when === 'feedback' ? feedbackInput : settledInput;
        const allowed = () => entry.session.program.follows.get(follow.name) === follow && follow.enabled.peek();
        const definition: ToolDefinition = { name: follow.name,
            description: follow.description + (follow.mode === 'background'
                ? '\n登记后台反馈关系后当前工作仍可继续；不重新发起原操作。'
                : '\n登记并等待原操作反馈，暂缓当前工作的自动推进；不重新发起原操作。'),
            isConcurrencySafe: () => true,
            parameters: toolSchema(input), output: { schema: toolSchema(z.object({ participationId: z.string(), when: z.enum(['settled', 'feedback']),
                    mode: z.enum(['wait', 'background']) }).strict(), 'output'),
                render: (_args, result) => [{ type: 'text', text: JSON.stringify(result) }] },
            execute: async (raw, exec) => {
                entry.session.program.flush();
                exec.signal.throwIfAborted();
                if (!allowed())
                    throw new Error('REACTIVE_CAPABILITY_UNAVAILABLE: ' + follow.name);
                let after: string | null = null;
                if (follow.when === 'feedback')
                    after = feedbackInput.parse(raw).after;
                else
                    settledInput.parse(raw);
                const execution = this.authority.toolExecution(entry, exec,
                    (continuation, wait) => this.operations.registerWait(entry.agent, continuation, wait));
                const id = await this.operations.participate(entry, follow, execution, after, allowed);
                exec.signal.throwIfAborted();
                if (follow.mode === 'wait')
                    exec.concludeTurn();
                return { participationId: id, when: follow.when, mode: follow.mode };
            },
        };
        definitions.set(follow, definition);
        return definition;
    }
    installPolicies(): void {
        this.ctx.effect(() => () => this.#goalDispatch.disable());
        this.ctx.on('tools/execute', (exec, next) => {
            if (exec.name !== 'create_goal' && exec.name !== 'update_goal') return next();
            const entry = exec.agent && this.sessions.forAgent(exec.agent);
            if (!entry) return next();
            const occurrence = entry.session.state.call(exec.callId);
            const call = occurrence && entry.session.state.value.calls[String(occurrence.callSequence)];
            const open = entry.session.state.value.openStep;
            if (!call || call.name !== exec.name || call.rootCallId !== exec.rootCallId
                || !open || open.turn !== call.turn || open.step !== call.step
                || entry.progress.goal(call.turn, call.step) === undefined)
                throw new Error('REACTIVE_GOAL_CONTROL_STEP_NOT_ADMITTED');
            const dispatch: GoalDispatch = { entry, exec, name: exec.name, callId: exec.callId, rootCallId: exec.rootCallId,
                callSequence: call.callSequence, turn: call.turn, step: call.step, signal: exec.signal, active: true };
            return this.#goalDispatch.run(dispatch, async () => {
                try { return await next(); }
                finally { dispatch.active = false; }
            });
        });
        this.ctx.on('goal/changed', ({ agent, change }) => {
            const dispatch = this.#goalDispatch.getStore();
            if (!dispatch?.active) return;
            const { entry, exec } = dispatch;
            if (exec.agent !== agent || entry.agent !== agent || exec.name !== dispatch.name
                || exec.callId !== dispatch.callId || exec.rootCallId !== dispatch.rootCallId
                || dispatch.signal.aborted || exec.signal.aborted || entry.scope.signal.aborted
                || entry.session.program.signal.aborted || this.ctx.agents.currentInitiator() !== agent
                || this.ctx.agents.get(agent.id) !== agent || this.sessions.forAgent(agent) !== entry
                || entry.session.live.value !== entry) return;
            const call = entry.session.state.value.calls[String(dispatch.callSequence)];
            const open = entry.session.state.value.openStep;
            if (!call || call.callId !== dispatch.callId || call.rootCallId !== dispatch.rootCallId || call.name !== dispatch.name
                || call.turn !== dispatch.turn || call.step !== dispatch.step || !open || open.turn !== dispatch.turn
                || open.step !== dispatch.step || entry.progress.goal(dispatch.turn, dispatch.step) === undefined) return;
            // 当前值只核验此原生事件仍为权威；签发的 ref 始终取事件，拒绝同步重入后的旧通知。
            const current = this.ctx.goals.get(agent), goal = change.goal;
            if (change.operation === 'clear') {
                if (current !== undefined) return;
            } else if (!goal || goal.id !== change.ref.id || goal.revision !== change.ref.revision
                || current?.id !== change.ref.id || current.revision !== change.ref.revision) return;
            const continues = ['create', 'edit', 'resume'].includes(change.operation) && goal?.phase === 'active';
            entry.progress.controlledGoal(dispatch.turn, dispatch.step,
                continues ? Object.freeze({ id: change.ref.id, revision: change.ref.revision }) : null);
        });
        this.ctx.on('tools/pre-execute', async (exec, next) => {
            const entry = exec.agent && this.sessions.forAgent(exec.agent);
            if (!entry)
                return next();
            const program = entry.session.program;
            program.flush();
            const policy = program.nativeTools[exec.name];
            if (!policy)
                return next();
            if (!this.nativeToolEnabled(entry, exec.name))
                return { kind: 'deny', reason: 'REACTIVE_CAPABILITY_UNAVAILABLE: ' + exec.name };
            if (this.ctx.agents.get(entry.agent.id) !== exec.agent || entry.session.live.value !== entry)
                return { kind: 'deny', reason: 'REACTIVE_NATIVE_TOOL_POLICY_CHANGED: ' + exec.name };
            this.workspaces.assertMember(entry.agent.session, entry.session.workspace, entry.session.environment);
            this.sessions.assertSessionWorkspace(entry.session);
            const occurrence = entry.session.state.call(exec.callId);
            if (!occurrence || occurrence.rootCallId !== exec.rootCallId)
                throw new Error('REACTIVE_CALL_INPUT_UNAVAILABLE');
            const changed = new AbortController();
            const lifetime = AbortSignal.any([exec.signal, program.signal, entry.scope.signal, entry.session.scope.signal, changed.signal]);
            try {
                lifetime.throwIfAborted();
                const qualification = policy.eligibility === undefined ? undefined : retainEligibility(exec.name, policy.eligibility, lifetime);
                const signal = qualification === undefined ? lifetime : AbortSignal.any([lifetime, qualification.signal]);
                let stop: (() => void) | undefined;
                this.#nativeAdmissions.set(exec, { entry, admit: policy.admit, eligibility: policy.eligibility, qualification, signal, stop: () => stop?.() });
                stop = entry.scope.effect(() => {
                    program.revision.get();
                    const current = program.nativeTools[exec.name];
                    // 贡献合并后的政策按显式开关、授权函数与共享事实来源判身份。
                    if (!current?.enabled || current.admit !== policy.admit || current.eligibility !== policy.eligibility)
                        changed.abort(new Error('REACTIVE_NATIVE_TOOL_POLICY_CHANGED: ' + exec.name));
                });
                qualification?.check();
                signal.throwIfAborted();
                await firstValueFrom(abortable(() => policy.admit?.({ name: exec.name,
                    arguments: z.json().parse(exec.arguments), sessionId: entry.agent.id,
                    workspaceId: entry.session.workspace.native.id, directory: entry.session.workspace.native.path,
                    source: { kind: 'tool', callId: exec.callId, rootCallId: exec.rootCallId, parentCallId: occurrence.parentCallId },
                    inputs: structuredClone(occurrence.inputs), signal }), signal));
                signal.throwIfAborted();
                qualification?.check();
            } catch (error) {
                this.releaseNativeAdmission(exec);
                if (exec.signal.aborted) return { kind: 'cancel' };
                throw error;
            }
            // 下游gate及原生ask仍由各自owner等待；最终guard重验本次准入，不能把旧allow交给新政策。
            try { return await next(); }
            catch (error) { this.releaseNativeAdmission(exec); throw error; }
        });
        this.ctx.on('tools/result', exec => { this.releaseNativeAdmission(exec); });
        this.ctx.tools.guard(exec => {
            const admission = this.#nativeAdmissions.get(exec);
            try {
                if (!exec.agent)
                    return;
                const entry = this.sessions.forAgent(exec.agent);
                if (admission) {
                    admission.signal.throwIfAborted();
                    if (!entry || entry !== admission.entry || this.ctx.agents.get(exec.agent.id) !== exec.agent
                        || entry.session.live.value !== entry)
                        return 'REACTIVE_NATIVE_TOOL_POLICY_CHANGED: ' + exec.name;
                }
                if (!entry)
                    return;
                entry.scope.assertActive();
                entry.session.program.signal.throwIfAborted();
                this.workspaces.assertMember(entry.agent.session, entry.session.workspace, entry.session.environment);
                entry.session.program.flush();
                admission?.signal.throwIfAborted();
                const policy = entry.session.program.nativeTools[exec.name];
                if (policy && (!admission || !policy.enabled || policy.admit !== admission.admit || policy.eligibility !== admission.eligibility))
                    return 'REACTIVE_NATIVE_TOOL_POLICY_CHANGED: ' + exec.name;
                if (admission && !policy)
                    return 'REACTIVE_NATIVE_TOOL_POLICY_CHANGED: ' + exec.name;
                admission?.qualification?.check();
                const offer = entry.session.program.offers.get(exec.name);
                const follow = entry.session.program.follows.get(exec.name);
                if (follow)
                    return follow.enabled.peek() ? undefined : 'REACTIVE_CAPABILITY_UNAVAILABLE: ' + exec.name;
                if (!offer)
                    return this.nativeToolEnabled(entry, exec.name) ? undefined : 'REACTIVE_CAPABILITY_NOT_DECLARED: ' + exec.name;
                if (!offer.enabled.peek())
                    return 'REACTIVE_CAPABILITY_UNAVAILABLE: ' + exec.name;
            } finally { this.releaseNativeAdmission(exec); }
        });
    }
}
