import { mkdir, mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createElement as h, Fragment } from 'react';
import { of, tap, type Observable } from 'rxjs';
import { LlmAdapter, type GenerateOptions, type StreamChunk } from '@deepseek-ai/dsh-llm';
import type { Context as NativeContext } from '@deepseek-ai/cordis';
import { SessionId } from '@deepseek-ai/dsh-session';
import { Context, Expose, Offer, implementProgram, useAction, useObservable, useRelation } from '@hanger-source/dsh-reactive/program';
import { defineAction, implementAction, defineProgram, defineSurface, defineApplication, defineEnvironment, nativeChildWorkAction, z, type ActionCallContext, type ActionInvocation, type InputReference } from '@hanger-source/dsh-reactive';
import { createRuntime, nativeModel, type DelegationAdmission, type RuntimeOptions } from '@hanger-source/dsh-reactive-dsh';

export const delegationDate = '2026-10-27T00:00:00.000Z';
export const delegationScope = 'business-parent';
export const DelegateWork = defineAction({ name: 'delegate_work', description: '委托独立子程序',
  scope: z.string().transform(parentId => ({ parentId })),
  input: z.object({ at: z.string().transform(value => new Date(value)), job: z.string() }).strict(),
  ...nativeChildWorkAction,
});
const CompleteChild = defineAction({ name: 'child_complete', description: '完成子工作自己的 Goal',
  scope: z.null(), input: z.object({}).strict(), output: z.boolean() });
const ParentOnly = defineAction({ name: 'parent_only_tool', description: '只属于父现场', scope: z.null(), input: z.null(), output: z.boolean() });
const CompleteParent = defineAction({ name: 'parent_complete', description: '完成原父 Goal', scope: z.null(), input: z.object({}).strict(), output: z.boolean() });
const childInput = z.object({ taskId: z.string(), issuedAt: z.string().transform(value => new Date(value)) }).strict();
export const ChildProgram = defineProgram({ name: 'delegation_child', input: childInput, surface: defineSurface({ views: {}, actions: {} }) });
export const ParentProgram = defineProgram({ name: 'delegation_parent', input: z.object({ parentId: z.string() }),
  surface: defineSurface({ views: {}, actions: { delegate: DelegateWork } }) });

export interface DelegationFixtureOptions {
  readonly root?: string;
  readonly parentSessionId?: string;
  readonly subagents?: RuntimeOptions['subagents'];
  readonly declaration$?: Observable<boolean>;
  respond(request: GenerateOptions): AsyncIterable<StreamChunk>;
  installNative?(native: NativeContext): void | Promise<void>;
  authorize?(admission: DelegationAdmission<typeof DelegateWork>): void | PromiseLike<void>;
  onInvocation?(invocation: ActionInvocation<typeof DelegateWork>): void;
  onChildComplete?(call: ActionCallContext<null>): void;
}

/** 正式 Runtime 与原生 provider；只在供应商模型边界注入确定性适配器。 */
export async function bootDelegationFixture(mode: 'spawn' | 'fork', options: DelegationFixtureOptions) {
  const declaration$ = options.declaration$ ?? of(true);
  const root = options.root ?? await mkdtemp(join(tmpdir(), 'dsh-reactive-delegation-' + mode + '-'));
  const directory = join(root, 'workspace');
  await mkdir(directory, { recursive: true });
  const errors: unknown[] = [];
  const invocations: ActionInvocation<typeof DelegateWork>[] = [];
  const admissions: DelegationAdmission<typeof DelegateWork>[] = [];
  const childInputs = new Map<string, readonly InputReference<z.output<typeof childInput>>[]>();
  const childComplete = implementAction(CompleteChild, { execute(_input, call) {
    call.completeGoal(); options.onChildComplete?.(call); return true;
  } });
  const childImplementation = implementProgram(ChildProgram, { Component({ conversation }) {
    useRelation(() => conversation.inputs$.pipe(tap(inputs => {
      if (inputs.length) childInputs.set(conversation.identity.sessionId, inputs);
    })), [conversation]);
    const complete = useAction(childComplete, () => null, []);
    return h(Fragment, null, h(Context, { name: 'child-only', value: { marker: 'CHILD_ONLY_CONTEXT' } }),
      Offer({ name: 'child_complete', action: complete }));
  } });
  const parentOnly = implementAction(ParentOnly, { execute: () => true });
  const parentComplete = implementAction(CompleteParent, { execute(_input, call) { call.completeGoal(); return true; } });
  const baseModel = nativeModel({ provider: 'delegation-' + mode, model: 'deterministic', create: () => new class extends LlmAdapter {
    stream(request: GenerateOptions) { return options.respond(request); }
  }() });
  const runtime = await createRuntime(assembly => {
    const delegation = assembly.implementDelegation(DelegateWork, { mode, program: childImplementation,
      childInput: input => ({ taskId: input.job, issuedAt: input.at.toISOString() }),
      objective: input => '完成独立任务 ' + input.job,
      authorize(admission) { admissions.push(admission); return options.authorize?.(admission); },
    });
    const implementation = implementProgram(ParentProgram, { Component() {
      const declared = useObservable(() => declaration$, []);
      const enabled = declared.status === 'ready' && declared.value;
      const action = useAction(delegation, () => delegationScope, []);
      useRelation(() => action.invocations$.pipe(tap(invocation => {
        invocations.push(invocation); options.onInvocation?.(invocation);
      })), [action]);
      const only = useAction(parentOnly, () => null, []);
      const complete = useAction(parentComplete, () => null, []);
      return h(Fragment, null, h(Context, { name: 'parent-only', value: { marker: 'PARENT_ONLY_SECRET' } }),
        Offer({ name: 'parent_only_tool', action: only }), Offer({ name: 'parent_complete', action: complete }),
        Offer({ name: 'delegate_work', action, enabled, follow: { when: 'feedback', scope: { parentId: delegationScope } } }),
        enabled ? Expose({ surface: ParentProgram.surface, value: { actions: { delegate: action } } }) : null);
    } });
    return defineApplication({ name: 'delegation-' + mode, admit: async () => {}, environments: [defineEnvironment({
      id: 'work', instructions: '父工作只依据当前动态现场', program: implementation,
    })] });
  }, { home: join(root, 'home'), actorId: 'original-human', serviceActorId: 'publisher', tools: 'native', goalRounds: 4, subagents: options.subagents,
    onError: event => { errors.push(event.error); },
    model: { ...baseModel, async install(native, home) { await baseModel.install(native, home); await options.installNative?.(native); } },
  });
  const workspace = await runtime.application.createWorkspace({ directory });
  const parent = options.parentSessionId ? await runtime.application.open(options.parentSessionId, ParentProgram)
    : await runtime.application.create('work', { workspaceId: workspace.workspaceId });
  return { root, runtime, workspace, parent, parentProgram: ParentProgram, childProgram: ChildProgram, delegate: DelegateWork,
    rawInput: { at: delegationDate, job: 'ai-job' }, rawScope: delegationScope, errors, invocations, admissions, childInputs,
    dispose: () => runtime.dispose() };
}

export async function inspectDelegationSession(native: NativeContext, id: string) {
  const handle = await native.sessionPersistence.open(SessionId(id), 'read');
  try { return { header: handle.header, inheritedEventCount: handle.inheritedEventCount, events: (await handle.read(0)).events }; }
  finally { await handle.close(); }
}
