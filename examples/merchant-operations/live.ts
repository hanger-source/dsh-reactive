import { mkdir, writeFile } from 'node:fs/promises';
import { isAbsolute, join } from 'node:path';
import { concatMap, filter, firstValueFrom, fromEvent, map, merge, of, startWith, throwError } from 'rxjs';
import { defineApplication, defineEnvironment, z, type GoalActivity } from '@hanger-source/dsh-reactive';
import { createRuntime, type ConversationHandle, type RuntimeModel } from '@hanger-source/dsh-reactive-dsh';
import { createMerchantBindings, createMerchantOwners, merchantOperationOwners } from './host.ts';
import { createMerchantPrograms } from './program.tsx';
import { merchantProgramInputSchema, OperationsProgram } from './program-contracts.ts';
import { createLocalMerchantOwner, type LocalDocument } from './live-owner.ts';

export type { LocalDocument } from './live-owner.ts';
export interface LiveMerchantOptions {
  /** 必须是尚不存在的隔离绝对目录；不读取或写入正式运营Instance。 */
  readonly directory: string;
  /** 宿主提供实际provider；此入口不接收、保存或打印apiKey。 */
  readonly runtimeModel: RuntimeModel;
  readonly documents: readonly LocalDocument[];
  readonly maxModelRequests?: number;
  readonly timeoutMs?: number;
  readonly signal?: AbortSignal;
  readonly onTrace?: (event: { readonly event: string; readonly sequence?: number; readonly sessionId?: string; readonly participationId?: string; readonly status?: string }) => void;
}

/**
 * 完整运行现有四域组件的本地资料协作。SQLite/文件属于此隔离业务owner，
 * enabled只表示本地接待配置文件可读，不表示生产商家部署、客户送达或外部服务上线。
 */
export async function runMerchantCollaboration(options: LiveMerchantOptions) {
  if (!isAbsolute(options.directory)) throw new Error('LIVE_DIRECTORY_NOT_ABSOLUTE');
  const maximum = options.maxModelRequests ?? 20, timeoutMs = options.timeoutMs ?? 600_000;
  if (!Number.isSafeInteger(maximum) || maximum < 1 || !Number.isSafeInteger(timeoutMs) || timeoutMs < 1) throw new Error('LIVE_BUDGET_INVALID');
  options.signal?.throwIfAborted();
  await mkdir(options.directory);
  const workspace = join(options.directory, 'workspace'); await mkdir(workspace);
  const documents = z.object({ name: z.string(), at: z.iso.datetime(), content: z.string() }).strict().array().parse(options.documents);
  const owner = await createLocalMerchantOwner(workspace, documents);
  try {
  const lifetime = new AbortController();
  const signal = AbortSignal.any([lifetime.signal, AbortSignal.timeout(timeoutMs), ...(options.signal ? [options.signal] : [])]);
  const method = join(options.directory, 'methods', 'local-merchant-collaboration'); await mkdir(method, { recursive: true });
  const dates = documents.map(item => Date.parse(item.at)).sort((left, right) => left - right);
  const startAt = new Date(dates[0]!).toISOString(), endAt = new Date(dates.at(-1)!).toISOString();
  const methodContent = [
    '# 本地资料协作', '',
    '这是文件与SQLite拥有事实的隔离业务，不是线上商家、FAI部署或客户接待。观察目标仅可读，不能替它提交任何动作。',
    '依据已注入的本方法及当前Context选择正确目标和当前修订，不猜操作ID、命令ID或updatedAt。正式参数来自当前领域事实；错误后读原事实，不重发未知受理。',
    '资料准备会生成manifest但先停在awaiting-command；需要沿原准备操作执行inspect命令核对实际源文件。inspect成功才是准备聚合prepared，单命令回执不等于聚合完成。',
    `知识来源是此工作区的本地历史资料消息，完整窗口为${startAt}至${endAt}。用historical-consultations来源合同索引该完整窗口；任务终态后仍须明确commit当前知识snapshot。`,
    '只有prepared、知识ready且snapshotReasonCode为空、正式snapshot已固定后，接待配置能力才可用。evaluation只支持skip，不声称完成评测。',
    '接待配置写入reception.json。最后商家命令只允许shell命令node verify-reception.mjs；它在此工作区启动实际Node子进程核验文件与知识摘要，不能执行其它Shell或发布/重启服务。',
    '每个Operation通过可靠反馈交回原工作，不轮询模型等待、不自动选择后续动作。回执与正式完成不同；只沿原操作和当前事实继续。',
    '读到最后核验命令completed、exitCode=0并核对输出摘要后，读取get_goal取得原Goal的精确id/revision，再按已证实的完整结果完成原Goal。不要把准备、任务或接待单项反馈直接当作整项工作完成。',
  ].join('\n');
  await writeFile(join(method, 'SKILL.md'), [
    '---', 'name: local-merchant-collaboration', 'description: 在隔离本地资料工作区完成准备、索引、固定知识与接待配置核验', '---', '', methodContent,
  ].join('\n'), { flag: 'wx' });
  const methodInstructions = `<skill_instructions>\n${methodContent}\n</skill_instructions>`;
  const contextSnapshot = z.object({ kind: z.literal('runtime-context'), form: z.literal('snapshot'),
    sections: z.object({ name: z.string(), text: z.string() }).array() });
  const contextSource = contextSnapshot.pick({ kind: true, form: true });
  const owners = createMerchantOwners(owner.owners);
  const bindings = createMerchantBindings(owner.services, owners, {
    requiredSkills: ['local-merchant-collaboration'], humanInput: { admitResponder() { throw new Error('LIVE_HUMAN_INPUT_NOT_AUTHORIZED'); } },
  });
  const programs = createMerchantPrograms(bindings, () => of([owner.target, owner.observer]));
  let modelRequests = 0;
  const skillPreparedRequests: number[] = [];
  const runtimeModel: RuntimeModel = { provider: options.runtimeModel.provider, model: options.runtimeModel.model,
    async install(ctx, home) {
      await options.runtimeModel.install(ctx, home);
      ctx.on('llm/stream', async function* (request, next) {
        signal.throwIfAborted();
        if (modelRequests === maximum) {
          const error = new Error('LIVE_MODEL_REQUEST_BUDGET_EXCEEDED'); lifetime.abort(error); throw error;
        }
        modelRequests++;
        if (request.purpose === undefined) {
          const latestContext = request.messages.findLast(message => message.role === 'user'
            && contextSource.safeParse(message.source).success);
          const snapshot = contextSnapshot.safeParse(latestContext?.source);
          const methodSection = snapshot.success
            ? snapshot.data.sections.find(section => section.name === 'reactive:skill:local-merchant-collaboration') : undefined;
          const actualText = latestContext?.content.filter(block => block.type === 'text').map(block => block.text).join('\n');
          if (!methodSection || !methodSection.text.includes(methodInstructions) || !actualText?.includes(methodSection.text)) {
            const error = new Error('LIVE_SKILL_NOT_PREPARED'); lifetime.abort(error); throw error;
          }
          skillPreparedRequests.push(modelRequests);
        }
        options.onTrace?.({ event: 'model-request', sequence: modelRequests, ...request.sessionId ? { sessionId: request.sessionId } : {} });
        // 唯一实际provider仍拥有请求与响应，不改内容、不预排工具或伪造模型输出。
        yield* next();
      });
    },
  };
  const definition = defineApplication({ name: 'local-merchant-collaboration',
    async admit(input, _identity, admission) {
      const scope = merchantProgramInputSchema.parse(input.scope), selected = scope.merchant, expected = owner.target.merchant;
      const participantAllowed = admission.participant === 'human' && admission.actorId === 'local-operator'
        || admission.participant === 'service' && admission.actorId === 'local-publisher';
      if (!participantAllowed || scope.onboardingId !== owner.target.onboardingId
        || selected.subject.kind !== expected.subject.kind || selected.subject.id !== expected.subject.id
        || selected.sandboxId !== expected.sandboxId || selected.deliveryIdentity !== expected.deliveryIdentity)
        throw new Error('LIVE_INPUT_DENIED');
    },
    environments: [defineEnvironment({ id: 'operations', skills: [method],
      instructions: '在当前工作区依据已注入的local-merchant-collaboration方法独立完成本地资料协作，只按Context的实际身份、能力和修订行动。对observer-only只观察，不办理。',
      program: programs.operations })],
  });
  const runtime = await createRuntime(() => definition, { home: join(options.directory, 'dsh'), model: runtimeModel,
      actorId: 'local-operator', serviceActorId: 'local-publisher', operations: merchantOperationOwners(bindings), tools: 'ptc', goalRounds: 2,
      onError: event => lifetime.abort(event.error),
    });
  try {
    let session: ConversationHandle<typeof OperationsProgram> | undefined;
    const stopped = () => { void session?.cancel().catch(error => options.onTrace?.({ event: 'cancel-failed', status: error instanceof Error ? error.name : 'unknown' })); };
    signal.addEventListener('abort', stopped, { once: true });
    try {
    const registration = await runtime.application.createWorkspace({ directory: workspace });
    session = await runtime.application.create('operations', { workspaceId: registration.workspaceId });
    const current = session, host = runtime.application;
    const aborted$ = fromEvent(signal, 'abort').pipe(concatMap(() => throwError(() => signal.reason)));
    signal.throwIfAborted();
    await firstValueFrom(merge(current.collection('merchants').instances$.pipe(filter(keys => keys.length === 2)), aborted$));
    // 原业务outbox保存link；这里仅把通知交给框架，未知或pending保持原记录等待真实资料变化。
    let notifying: Promise<void> = Promise.resolve();
    const notifications = owner.changes$.pipe(startWith(undefined), concatMap(() => notifying = (async () => {
      for (const pending of owner.pending()) {
        const result = await host.notifyOperation(pending.link.continuation.sessionId, pending.id, { actorId: 'local-publisher', signal });
        options.onTrace?.({ event: 'operation-notification', participationId: pending.id, status: result.status, sessionId: pending.link.continuation.sessionId });
        if (result.status === 'accepted' || result.status === 'not-started') owner.acknowledge(pending.id);
      }
    })())).subscribe({ error: error => lifetime.abort(error) });
    try {
    const terminal = merge(
      current.activity$.pipe(map(activity => activity.goal), filter((goal): goal is GoalActivity => !!goal && ['complete', 'blocked'].includes(goal.phase))),
      aborted$,
    );
    const ended = firstValueFrom(terminal); void ended.catch(() => {});
    signal.throwIfAborted();
    await current.submit({ requestId: 'local-document-collaboration', intent: 'goal', delivery: 'followup', scope: owner.target,
      content: `请依据已注入的工作方法，为local-documents完成这一批本地资料准备、完整窗口索引、知识snapshot固定和本地接待配置，并运行最后的文件核验，依据实际结果结束本工作。资料窗口为${startAt}至${endAt}。observer-only目标只读。本次不要求线上商家部署、正式接待、客户送达或离线评测。`,
    }, { actorId: 'local-operator', signal });
    const goal = await ended;
    if (goal.phase !== 'complete') throw new Error('LIVE_GOAL_BLOCKED');
    await current.whenIdle(); signal.throwIfAborted();
    notifications.unsubscribe(); await notifying; signal.throwIfAborted();
    const evidence = await owner.verify();
    signal.throwIfAborted();
    if (!skillPreparedRequests.length) throw new Error('LIVE_SKILL_NOT_PREPARED');
    const report = { schema: 'reactive.local-merchant-collaboration.v1', directory: options.directory, sessionId: current.id,
      workspaceId: registration.workspaceId, model: { provider: runtimeModel.provider, model: runtimeModel.model }, modelRequests, skillPreparedRequests,
      target: owner.target, observer: owner.observer, goal: { id: goal.id, revision: goal.revision, phase: goal.phase }, ...evidence };
    signal.throwIfAborted();
    await writeFile(join(options.directory, 'result.json'), JSON.stringify(report, null, 2), { flag: 'wx', signal });
    signal.throwIfAborted();
    return report;
    } finally {
      notifications.unsubscribe();
      // RxJS退订不取消已交接的Promise；关数据库前等待当前通知真正退出。
      await notifying;
    }
    } finally {
      signal.removeEventListener('abort', stopped); session?.release();
    }
  } finally {
    await runtime.dispose(); signal.throwIfAborted();
  }
  } finally { owner.close(); }
}
