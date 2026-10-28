import type { Context } from "@deepseek-ai/cordis";
import type { Agent } from "@deepseek-ai/dsh-agent";
import type { ToolDefinition } from "@deepseek-ai/dsh-tools";
import { z } from "zod";
import { observeBoundAction } from "@hanger-source/dsh-reactive/kernel";
import { type BoundOffer, type BoundFollow } from "@hanger-source/dsh-reactive/kernel";
import { toolSchema } from "./schema.ts";
import { type Workspaces } from "../workspaces/workspaces.ts";
import type { LiveConversation } from '../conversations/session.ts';
import type { ConversationSessions } from '../conversations/sessions.ts';
import type { ActionAuthority } from './authority.ts';
import type { OperationParticipation } from '../operations/participation.ts';
const nativeTools = new Set(['run_code', 'skill', 'get_goal', 'create_goal', 'update_goal']);
/** 原生工具与 PTC 取得同一能力表；缓存仅属于真实 Agent 和声明绑定。 */
export class ProgramTools {
    #toolDefinitions = new WeakMap<Agent, WeakMap<BoundOffer | BoundFollow, ToolDefinition>>();
    constructor(private readonly ctx: Context, private readonly workspaces: Workspaces, private readonly sessions: ConversationSessions, private readonly authority: ActionAuthority, private readonly operations: OperationParticipation) { }
    nativeToolEnabled(entry: LiveConversation, name: string): boolean {
        const declared = entry.session.program.nativeTools[name];
        if (declared)
            return declared.enabled;
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
                return program.invoke(offer.action, raw, this.authority.call(entry, exec, offer), allowed).result;
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
                const execution = this.authority.toolExecution(entry, exec);
                const id = await this.operations.participate(entry, follow, execution, after);
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
        this.ctx.on('tools/pre-execute', async (exec, next) => {
            const entry = exec.agent && this.sessions.forAgent(exec.agent);
            if (!entry)
                return next();
            const program = entry.session.program;
            program.flush();
            const policy = program.nativeTools[exec.name];
            if (!policy)
                return next();
            if (!policy.enabled)
                return { kind: 'deny', reason: 'REACTIVE_CAPABILITY_UNAVAILABLE: ' + exec.name };
            this.sessions.assertSessionWorkspace(entry.session);
            const occurrence = entry.session.state.call(exec.callId);
            if (!occurrence || occurrence.rootCallId !== exec.rootCallId)
                throw new Error('REACTIVE_CALL_INPUT_UNAVAILABLE');
            await policy.admit?.({ name: exec.name, arguments: z.json().parse(exec.arguments), sessionId: entry.agent.id,
                workspaceId: entry.session.workspace.native.id, directory: entry.session.workspace.native.path,
                source: { kind: 'tool', callId: exec.callId, rootCallId: exec.rootCallId, parentCallId: occurrence.parentCallId },
                inputs: structuredClone(occurrence.inputs), signal: exec.signal });
            exec.signal.throwIfAborted();
            program.flush();
            const current = program.nativeTools[exec.name];
            if (!current?.enabled || current.admit !== policy.admit)
                return { kind: 'deny', reason: 'REACTIVE_NATIVE_TOOL_POLICY_CHANGED: ' + exec.name };
            return next();
        });
        this.ctx.tools.guard(exec => {
            if (!exec.agent)
                return;
            const entry = this.sessions.forAgent(exec.agent);
            if (!entry)
                return;
            this.workspaces.assertMember(entry.agent.session, entry.session.workspace, entry.session.environment);
            entry.session.program.flush();
            const offer = entry.session.program.offers.get(exec.name);
            const follow = entry.session.program.follows.get(exec.name);
            if (follow)
                return follow.enabled.peek() ? undefined : 'REACTIVE_CAPABILITY_UNAVAILABLE: ' + exec.name;
            if (!offer)
                return this.nativeToolEnabled(entry, exec.name) ? undefined : 'REACTIVE_CAPABILITY_NOT_DECLARED: ' + exec.name;
            if (!offer.enabled.peek())
                return 'REACTIVE_CAPABILITY_UNAVAILABLE: ' + exec.name;
        });
    }
}
