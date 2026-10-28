import { BehaviorSubject, ReplaySubject, Subject, takeWhile, type Observable } from 'rxjs';
import { z } from 'zod';
import { ResourceReader } from '../resources/reader.ts';
import type { Observation } from '../resources/observation.ts';
import type { Continuation, InputReference, Json, WaitReference } from '../conversations/protocol.ts';
import { humanPromptsSchema, humanAskOptionsSchema, humanAskResultSchema, type HumanPrompt, type HumanAskOptions, type HumanAnswer, type HumanAskResult } from '../participation/contract.ts';
import { attachOperation, isOperationOwner, locateOperation, operationCommand, type OperationContract, type OperationHandle, type OperationLocation, type OperationOwner, type OperationOwnerReference, type OperationReference, type OperationState } from '../operations/contract.ts';

function isPromiseLike<T>(value: T | PromiseLike<T>): value is PromiseLike<T> {
  return value !== null && (typeof value === 'object' || typeof value === 'function')
    && typeof Reflect.get(value, 'then') === 'function';
}

/** 原生身份仅由真实工具入口提供；普通参与者不声明原生调用身份。 */
export type ActionSource =
  | { readonly kind: 'tool'; readonly callId: string; readonly rootCallId: string; readonly parentCallId: string | null }
  | { readonly kind: 'participant'; readonly actorId: string; readonly participant: 'human' | 'service'; readonly entrypoint: string };

/**
 * 宿主为本次调用建立的可信执行关联；绑定的 admit 仍须核验其真实来源。
 * invocationId 是调用关联，不是授权凭证，也不承担重试去重。
 */
export interface ActionExecutionContext {
  readonly invocationId: string;
  readonly sessionId: string;
  readonly source: ActionSource;
  readonly inputs: readonly InputReference[];
  readonly signal: AbortSignal;
  readonly continuation: Continuation | null;
  /** 宿主登记原业务等待；只有实际工具入口拥有结束自身 turn 的接线。 */
  readonly wait?: (reference: WaitReference) => Promise<void>;
  /** 宿主按本次关联的 Goal revision 核验完成，不选择当前最新目标。 */
  readonly completeGoal?: () => void;
  /** 只有真实原生工具来源提供；宿主必须传递此子工作 signal 并等待实际执行静止。 */
  readonly askHuman?: (questions: readonly HumanPrompt[], options: HumanAskOptions | undefined, signal: AbortSignal) => Promise<HumanAskResult>;
}

/** 一次业务执行取得的明确目标与真实调用来源。 */
export interface ActionCallContext<Scope> extends Omit<ActionExecutionContext, 'inputs' | 'continuation' | 'wait' | 'completeGoal' | 'askHuman'> {
  readonly inputs: ActionTarget<readonly InputReference[]>;
  readonly continuation: ActionTarget<Continuation> | null;
  readonly scope: Scope;
  wait(reference: WaitReference): Promise<void>;
  completeGoal(): void;
  /** 阻塞本次业务函数直到答复；取消不恢复函数栈。 */
  askHuman(questions: readonly HumanPrompt[]): Promise<HumanAnswer>;
  /** 超时返回 pending；晚答沿原生问题回到模型，不重新执行本次业务。 */
  askHuman(questions: readonly HumanPrompt[], options: HumanAskOptions): Promise<HumanAskResult>;
}

/** 绑定目标是不可变的 JSON 引用，不容纳业务服务或可变实体。 */
export type ReadonlyActionJson = string | number | boolean | null | readonly ReadonlyActionJson[] | { readonly [key: string]: ReadonlyActionJson };
export type ActionTarget<T> = Json extends T ? ReadonlyActionJson
  : T extends object ? { readonly [K in keyof T]: ActionTarget<T[K]> } : T;

/** 合同发布者声明的跨边界身份；不替代本地定义对象或绑定身份。 */
export interface ActionIdentity {
  readonly name: string;
  readonly version: number;
}
/** 可跨消费者共享的纯合同；不携带宿主业务实现。 */
export interface ActionContract extends ActionIdentity {
  readonly description: string;
  readonly scope: z.ZodType<Json>;
  readonly input: z.ZodType;
  readonly output: z.ZodType;
  readonly concurrency?: 'parallel' | 'serial';
  readonly observe?: (output: never) => Json;
  readonly operation?: ActionOperationContract;
}

export interface ActionOperationContract {
  readonly definition: OperationContract;
  command(input: never, scope: never, origin: ActionCommandOrigin): unknown;
  reference(receipt: never): unknown;
}
/** 命令关联可使用真实调用事实；它不授予取消、等待或 Goal 权限。 */
export interface ActionCommandOrigin {
  readonly invocationId: string;
  readonly sessionId: string;
  readonly source: ActionSource;
  readonly inputs: ActionTarget<readonly InputReference[]>;
}
type ActionOperationMapping<I extends z.ZodType, O extends z.ZodType, S extends z.ZodType<Json>, P extends OperationContract> = {
    readonly definition: P;
    command(input: z.output<NoInfer<I>>, scope: ActionTarget<z.output<NoInfer<S>>>, origin: ActionCommandOrigin): z.input<P['command']>;
    reference(receipt: z.output<NoInfer<O>>): z.input<P['reference']>;
  };
type OperationOf<Mapping> = Mapping extends ActionOperationContract ? Mapping['definition'] : never;
export type ActionOperation<D extends ActionContract> = OperationOf<NonNullable<D['operation']>>;

export type ActionInput<D extends ActionContract> = z.input<D['input']>;
export type ActionOutput<D extends ActionContract> = z.output<D['output']>;
export type ActionScope<D extends ActionContract> = ActionTarget<z.output<D['scope']>>;
export type ActionObserved<D extends ActionContract> = ReturnType<NonNullable<D['observe']>>;

export interface ActionOptions<I extends z.ZodType, O extends z.ZodType, S extends z.ZodType<Json>, Observed extends Json = never,
  P extends OperationContract = never> {
  readonly name: string;
  /** 跨进程和持久结果的合同版本；新定义省略时为 1。 */
  readonly version?: number;
  readonly description: string;
  readonly scope: S;
  readonly input: I;
  readonly output: O;
  /** 提供给原生调度器的声明；此处不另建串行或并发队列。 */
  readonly concurrency?: 'parallel' | 'serial';
  /** 公开观察只消费已解析输出；不运行第二次 output transform。 */
  observe?(output: z.output<NoInfer<O>>): Observed;
  readonly operation?: ActionOperationMapping<I, O, S, P>;
}

type ActionExecutor<D extends ActionContract> =
  (input: ActionParsedInput<D>, call: ActionCallContext<ActionScope<D>>) => z.input<D['output']> | PromiseLike<z.input<D['output']>>;

/** 宿主为纯合同提供业务实现；构造时不执行或注册业务。 */
export type ActionImplementationOptions<D extends ActionContract> = {
  execute: ActionExecutor<D>;
} & ([ActionOperation<D>] extends [never] ? { readonly operationOwner?: never }
  : { readonly operationOwner: OperationOwner<ActionOperation<D>> });

/** 实现只公开其原合同身份；实际 handler 由本模块私有身份持有。 */
export interface ActionImplementation<D extends ActionContract = ActionContract> {
  readonly definition: D;
}

/** 宿主统一准备/核对持久关联；业务 execute 不按入口处理 follow。 */
export type ActionOperationEvent =
  | { readonly phase: 'access'; readonly owner: OperationOwnerReference }
  | { readonly phase: 'prepare'; readonly owner: OperationOwnerReference; readonly command: Json }
  | { readonly phase: 'not-started'; readonly owner: OperationOwnerReference; readonly command: Json }
  | { readonly phase: 'receipt'; readonly owner: OperationOwnerReference; readonly command: Json; readonly reference: Json };

export interface ActionBindingOptions<D extends ActionContract> {
  /** 声明 owner 当前是否已提交并激活；临时退出不等同于永久 release。 */
  active?(): boolean;
  /** 程序普通调用的可信来源工厂；原生入口仍显式传入本次原生执行关联。 */
  execution?(action: ActionReference): ActionExecutionContext;
  /**
   * 每次调用在副作用之前核验宿主执行关联、权限和当前绑定。
   * 框架没有可由调用者自报而绕过该检查的授权标记。
   */
  admit(input: ActionParsedInput<D>, call: ActionCallContext<ActionScope<D>>, execution: ActionExecutionContext): void | PromiseLike<void>;
  /** @internal 原始 execution 身份继续由实际宿主核验。 */
  operation?(event: ActionOperationEvent, execution: ActionExecutionContext): void | PromiseLike<void>;
}

/** 业务 execute 与准入消费 input schema 的解析结果。 */
export type ActionParsedInput<D extends ActionContract> = z.output<D['input']>;

export type InvocationPhase = 'input' | 'admission' | 'operation' | 'execute' | 'output';
/** 业务行为的调用事实，不代表原生工具后置策略的最终结果。 */
export type InvocationActivity<Observed extends Json = Json> =
  | { readonly state: 'pending' }
  | { readonly state: 'running' }
  | { readonly state: 'returned'; readonly observation?: Observed; readonly observationError?: unknown }
  | { readonly state: 'failed'; readonly phase: InvocationPhase; readonly error: unknown };

export interface Invocation<Output, Observed extends Json = Json> {
  readonly id: string;
  readonly source: ActionSource;
  /** 显式调用时即启动；读取或订阅不会重新执行。 */
  readonly result: Promise<Output>;
  /** 本次调用的同源当前事实；read 不等待业务完成，业务失败仍是 ready 的调用事实。 */
  readonly activity: ResourceReader<InvocationActivity<Observed>>;
  /** 同一次执行的最近状态与更新；晚订阅仍能取得终态。 */
  readonly activity$: Observable<InvocationActivity<Observed>>;
}

export type ActionInvocation<D extends ActionContract> = Invocation<ActionOutput<D>, ActionObserved<D>>
  & ([ActionOperation<D>] extends [never] ? {} : { readonly operation: Promise<OperationHandle<ActionOperation<D>>> });

/** 异构声明只持有合同与目标；调用须通过实际绑定的内部身份分派。 */
export interface ActionReference {
  readonly definition: ActionContract;
  /** 一次真实绑定的身份；不授予执行权限，也不承担持久恢复。 */
  readonly bindingId: string;
  readonly scope: unknown;
  readonly released: boolean;
}

/** 捕获的绑定已撤下或被另一个真实绑定替换；拒绝本次新受理。 */
export class ActionBindingUnavailableError extends Error {
  override readonly name = 'ActionBindingUnavailableError';
  constructor(readonly actionName: string, readonly reason: 'not-exposed' | 'binding-changed') {
    super('Action binding is unavailable: ' + actionName);
  }
}

/** 解析入口取得的当前目标与消费者明确期待的目标不同。 */
export class ActionTargetMismatchError extends Error {
  override readonly name = 'ActionTargetMismatchError';
  readonly reason = 'target-mismatch';
  constructor(readonly actionName: string) { super('Action target does not match: ' + actionName); }
}

/** 绑定是普通可调用值，定义、目标和生存期属于同一个 owner。 */
type ActionExecutionArguments<Automatic extends boolean> = Automatic extends true
  ? [execution?: ActionExecutionContext] : [execution: ActionExecutionContext];
export interface BoundAction<D extends ActionContract, Automatic extends boolean = false> extends ActionReference {
  (input: ActionInput<D>, ...execution: ActionExecutionArguments<Automatic>): ActionInvocation<D>;
  readonly definition: D;
  readonly scope: ActionScope<D>;
  readonly released: boolean;
  /** 本绑定新建立的真实调用句柄；热观察不重放、不启动或取消执行。 */
  readonly invocations$: Observable<ActionInvocation<D>>;
  /** 本绑定当前尚未结算的调用；晚订阅取得当前集合，不保存终态历史。 */
  readonly activeInvocations$: Observable<readonly ActionInvocation<D>[]>;
  invoke(input: ActionInput<D>, ...execution: ActionExecutionArguments<Automatic>): ActionInvocation<D>;
  /** 明确重连已有操作；读取权限仍由当前宿主与业务 owner 核验。 */
  attach(reference: z.input<ActionOperation<D>['reference']>, ...execution: ActionExecutionArguments<Automatic>): OperationHandle<ActionOperation<D>>;
  /** 按业务 command 查受理事实，不重新 execute。 */
  locate(command: z.input<ActionOperation<D>['command']>, ...execution: ActionExecutionArguments<Automatic>):
    Promise<OperationLocation<OperationReference<ActionOperation<D>>, OperationState<ActionOperation<D>>>>;
  /** 纯观察投影，不执行行为，也不重新解析输出。 */
  observe(output: ActionOutput<D>): ActionObserved<D> | undefined;
  /** 仅拒绝后续受理，不取消已经开始的业务执行。 */
  release(): void;
}

/** 可共享的行为合同；客户端导入它不需要导入业务 owner。 */
export interface ActionDefinition<I extends z.ZodType, O extends z.ZodType, S extends z.ZodType<Json>, Observed extends Json = never,
  P extends OperationContract = never>
extends ActionContract {
  readonly name: string;
  readonly description: string;
  readonly scope: S;
  readonly input: I;
  readonly output: O;
  readonly concurrency?: 'parallel' | 'serial';
  readonly observe?: (output: z.output<O>) => Observed;
  readonly operation?: ActionOperationMapping<I, O, S, P>;
}

type DefinitionBinder<I extends z.ZodType, O extends z.ZodType, S extends z.ZodType<Json>, Observed extends Json, P extends OperationContract> = (
  rawScope: z.input<S>, options: ActionBindingOptions<ActionDefinition<I, O, S, Observed, P>>,
) => BoundAction<ActionDefinition<I, O, S, Observed, P>, true>;
const actionDefinitions = new WeakSet<object>();
const implementationBindings = new WeakMap<object, unknown>();
type InputCapture = (raw: unknown) => Json;
type ArgumentCapture = { readonly input: InputCapture; readonly scope: InputCapture };
const implementationArgumentCaptures = new WeakMap<object, ArgumentCapture>();
const invocationArguments = new WeakMap<ActionCallContext<unknown>, { readonly input: Json; readonly scope: Json }>();
const executionGuards = new WeakMap<ActionExecutionContext, () => void>();
type RawActionDispatcher = (raw: unknown, execution: ActionExecutionContext) => Invocation<unknown, Json>;
const boundDispatchers = new WeakMap<object, {
  readonly invoke: RawActionDispatcher;
  readonly operationOwner?: OperationOwnerReference;
  observe(output: unknown): Json | undefined;
}>();

/** @internal 只接受本模块实际创建的绑定，不由公开字段认定执行身份。 */
export function isBoundAction(reference: unknown): reference is ActionReference {
  return ((typeof reference === 'object' && reference !== null) || typeof reference === 'function')
    && boundDispatchers.has(reference);
}

/** @internal 供异构 Surface / 原生工具边界进入原绑定的唯一解析与执行入口。 */
export function invokeBoundAction(reference: ActionReference, raw: unknown, execution: ActionExecutionContext): Invocation<unknown, Json> {
  const dispatch = boundDispatchers.get(reference);
  if (!dispatch) throw new Error('REACTIVE_ACTION_BINDING_INVALID');
  return dispatch.invoke(raw, execution);
}

/** @internal 原生最终结果的纯展示投影；不重执行行为，也不重新解析 output。 */
export function observeBoundAction(reference: ActionReference, output: unknown): Json | undefined {
  const dispatch = boundDispatchers.get(reference);
  if (!dispatch) throw new Error('REACTIVE_ACTION_BINDING_INVALID');
  return dispatch.observe(output);
}

/** @internal 只读取实际绑定捕获的 owner，不从公开字段推断执行身份。 */
export function actionOperationOwner(reference: ActionReference): OperationOwnerReference | undefined {
  const dispatch = boundDispatchers.get(reference);
  if (!dispatch) throw new Error('REACTIVE_ACTION_BINDING_INVALID');
  return dispatch.operationOwner;
}

/** @internal 原生持久委托为自己的实际实现登记输入快照；普通 Action 不增加 JSON 输入限制。 */
export function captureActionArguments(implementation: ActionImplementation, capture: ArgumentCapture): void {
  if (!implementationBindings.has(implementation)) throw new Error('REACTIVE_ACTION_IMPLEMENTATION_INVALID');
  if (implementationArgumentCaptures.has(implementation)) throw new Error('REACTIVE_ACTION_ARGUMENT_CAPTURE_DUPLICATE');
  implementationArgumentCaptures.set(implementation, capture);
}

/** @internal 只读取本模块在调用开始保存的原始输入，不反向编码解析后的领域值。 */
export function actionInvocationArguments(call: ActionCallContext<unknown>): { readonly input: Json; readonly scope: Json } {
  const arguments_ = invocationArguments.get(call);
  if (!arguments_) throw new Error('REACTIVE_ACTION_ARGUMENTS_NOT_CAPTURED');
  return arguments_;
}

/** @internal 宿主为原始执行对象登记一次同步检查，在实际 execute 前核验当前权威。 */
export function guardActionExecution(execution: ActionExecutionContext, check: () => void): void {
  if (executionGuards.has(execution)) throw new Error('REACTIVE_ACTION_EXECUTION_GUARD_DUPLICATE');
  executionGuards.set(execution, check);
}

function createBinding<I extends z.ZodType, O extends z.ZodType, S extends z.ZodType<Json>, Observed extends Json, P extends OperationContract>(
  definition: ActionDefinition<I, O, S, Observed, P>,
  rawScope: z.input<S>,
  options: ActionBindingOptions<ActionDefinition<I, O, S, Observed, P>>,
  execute: ActionExecutor<ActionDefinition<I, O, S, Observed, P>>,
  operationOwner?: OperationOwnerReference,
  captureArguments?: ArgumentCapture,
): BoundAction<ActionDefinition<I, O, S, Observed, P>, true> {
    type Definition = ActionDefinition<I, O, S, Observed, P>;
    const bindingId = crypto.randomUUID();
    type Operation = ActionOperation<Definition>;
    // 目标只在绑定时解析，后续调用不把解析后的目标再次交回 raw schema。
    const captured = captureArguments ? { input: captureArguments.input,
      scope: ownJson(captureArguments.scope(rawScope), 'REACTIVE_ACTION_SCOPE_SNAPSHOT_NOT_JSON') } : undefined;
    const scope = ownJson(definition.scope.parse(captured ? captured.scope : rawScope), 'REACTIVE_ACTION_SCOPE_NOT_JSON');
    let released = false;
    const invocations = new Subject<ActionInvocation<Definition>>();
    const active = new Set<ActionInvocation<Definition>>();
    const activeInvocations = new ReplaySubject<readonly ActionInvocation<Definition>[]>(1);
    const publishActive = () => activeInvocations.next(Object.freeze([...active]));
    publishActive();
    const assertActive = () => {
      if (released) throw new Error('REACTIVE_ACTION_BINDING_RELEASED');
      if (options.active && !options.active()) throw new Error('REACTIVE_ACTION_BINDING_INACTIVE');
    };
    const observe = (output: z.output<O>) => definition.observe?.(output);
    const executionFor = (explicitExecution?: ActionExecutionContext) => {
      const execution = explicitExecution === undefined ? options.execution?.(bound) : explicitExecution;
      if (!execution) throw new Error('REACTIVE_ACTION_EXECUTION_REQUIRED');
      return execution;
    };
    const currentOperationOwner = (): OperationOwner<Operation> => {
      if (!operationOwner) throw new Error('REACTIVE_ACTION_OPERATION_UNAVAILABLE');
      // implementAction 已核验真实 owner 与同一纯 Operation 合同，擦除只发生在该闭包。
      return operationOwner as OperationOwner<Operation>;
    };
    const attach = (raw: z.input<Operation['reference']>, explicitExecution?: ActionExecutionContext): OperationHandle<Operation> => {
      const owner = currentOperationOwner();
      const execution = executionFor(explicitExecution);
      return attachOperation(owner, raw, execution, () => options.operation?.({ phase: 'access', owner }, execution));
    };
    const locate = (raw: z.input<Operation['command']>, explicitExecution?: ActionExecutionContext) => {
      const owner = currentOperationOwner();
      const execution = executionFor(explicitExecution);
      return locateOperation(owner, raw, execution, () => options.operation?.({ phase: 'access', owner }, execution));
    };
    const invoke = (rawInput: z.input<I>, explicitExecution?: ActionExecutionContext): ActionInvocation<Definition> => {
      const execution = executionFor(explicitExecution);
      const invocationId = execution.invocationId;
      const source = ownJson(execution.source, 'REACTIVE_ACTION_CONTEXT_NOT_JSON');
      const inputs = ownJson(execution.inputs, 'REACTIVE_ACTION_CONTEXT_NOT_JSON');
      const continuation = ownJson(execution.continuation, 'REACTIVE_ACTION_CONTEXT_NOT_JSON');
      const signal = execution.signal;
      const wait = execution.wait;
      const completeGoal = execution.completeGoal;
      const requestHuman = execution.askHuman;
      let bodyActive = false;
      const humanLifetime = new AbortController();
      const humanRequests = new Set<Promise<HumanAskResult>>();
      const assertControl = () => {
        if (!bodyActive) throw new Error('REACTIVE_ACTION_CONTROL_INACTIVE');
        signal.throwIfAborted();
      };
      function askHuman(questions: readonly HumanPrompt[]): Promise<HumanAnswer>;
      function askHuman(questions: readonly HumanPrompt[], options: HumanAskOptions): Promise<HumanAskResult>;
      function askHuman(questions: readonly HumanPrompt[], options?: HumanAskOptions): Promise<HumanAskResult> {
        let work: Promise<HumanAskResult>;
        try {
          assertControl();
          if (!requestHuman) throw new Error('REACTIVE_ACTION_HUMAN_INPUT_UNAVAILABLE');
          const prompts = humanPromptsSchema.parse(questions);
          const requested = options === undefined ? undefined : humanAskOptionsSchema.parse(options);
          work = Promise.resolve(requestHuman.call(execution, prompts, requested,
            AbortSignal.any([signal, humanLifetime.signal]))).then(value => {
            const result = humanAskResultSchema.parse(value);
            if (requested === undefined && 'pending' in result) throw new Error('REACTIVE_HUMAN_UNEXPECTED_PENDING');
            return result;
          });
        } catch (error) { work = Promise.reject(error); }
        humanRequests.add(work);
        // 本次业务函数拥有已发起的子问答；无人 await 也不会留下未处理拒绝或越过父结算。
        void work.then(() => humanRequests.delete(work), () => humanRequests.delete(work));
        return work;
      }
      const origin: ActionCommandOrigin = Object.freeze({ invocationId, sessionId: execution.sessionId, source, inputs });
      const activityState = new BehaviorSubject<Observation<InvocationActivity<Observed>>>({
        status: 'ready', generation: 1, observedAt: Date.now(), value: { state: 'pending' },
      });
      const activity = new ResourceReader<InvocationActivity<Observed>>({ peek: () => activityState.value,
        observations$: activityState.pipe(takeWhile(observation => observation.status === 'pending'
          || (observation.status === 'ready' && (observation.value.state === 'pending' || observation.value.state === 'running')), true)),
      });
      const publishActivity = (value: InvocationActivity<Observed>): void => activityState.next({
        status: 'ready', generation: activityState.value.generation + 1, observedAt: Date.now(), value,
      });
      const call: ActionCallContext<ActionTarget<z.output<S>>> = Object.freeze({
        invocationId,
        sessionId: execution.sessionId,
        source,
        inputs,
        signal,
        continuation,
        scope,
        wait(reference: WaitReference) {
          assertControl();
          if (!wait) throw new Error('REACTIVE_ACTION_WAIT_UNAVAILABLE');
          return wait.call(execution, reference);
        },
        completeGoal() {
          assertControl();
          if (!completeGoal) throw new Error('REACTIVE_ACTION_GOAL_COMPLETION_UNAVAILABLE');
          completeGoal.call(execution);
        },
        askHuman,
      });
      let attached: OperationHandle<Operation> | undefined;
      let finished = false, invocation: ActionInvocation<Definition> | undefined;
      const result = (async () => {
        let phase: InvocationPhase = 'input';
        let prepared: { readonly owner: OperationOwnerReference; readonly command: Json } | undefined;
        let started = false;
        try {
          assertActive();
          call.signal.throwIfAborted();
          // 捕获发生在首个异步准入之前；仅持久委托使用明确的 JSON 快照合同。
          let inputSource: unknown = rawInput;
          if (captured) {
            const snapshot = ownJson(captured.input(rawInput), 'REACTIVE_ACTION_INPUT_SNAPSHOT_NOT_JSON');
            invocationArguments.set(call, Object.freeze({ input: snapshot, scope: captured.scope }));
            inputSource = snapshot;
          }
          const input = definition.input.parse(inputSource);
          phase = 'admission';
          await options.admit(input, call, execution);
          // 等待准入期间退出的声明没有受理执行；已进入 execute 后不再依赖声明。
          assertActive();
          call.signal.throwIfAborted();
          let command: Json | undefined;
          const mapping = definition.operation;
          if (mapping && operationOwner) {
            phase = 'operation';
            command = operationCommand(operationOwner, mapping.command(input, scope, origin));
            prepared = { owner: operationOwner, command };
            await options.operation?.({ phase: 'prepare', owner: operationOwner, command }, execution);
            // 持久准备也属于受理之前；退出声明不允许在等待之后开始新副作用。
            assertActive();
            call.signal.throwIfAborted();
          }
          // 准入和持久准备的所有异步等待结束后，在副作用开始的同一同步边界核验宿主权威。
          executionGuards.get(execution)?.();
          publishActivity({ state: 'running' });
          phase = 'execute';
          let rawOutput: z.input<O>;
          try {
            bodyActive = true;
            started = true;
            const returned = execute(input, call);
            rawOutput = isPromiseLike(returned) ? await returned : returned;
          } finally {
            bodyActive = false;
            humanLifetime.abort(new Error('REACTIVE_ACTION_CONTROL_INACTIVE'));
            await Promise.allSettled([...humanRequests]);
          }
          call.signal.throwIfAborted();
          phase = 'output';
          const output = definition.output.parse(rawOutput);
          if (mapping && operationOwner && command !== undefined) {
            phase = 'operation';
            // 映射捕获具体 Operation 合同；擦除的 raw 随即进入该 owner 的唯一 reference 解析。
            const handle = attachOperation(currentOperationOwner(),
              mapping.reference(output) as z.input<Operation['reference']>, call);
            const reference = handle.reference;
            await options.operation?.({ phase: 'receipt', owner: operationOwner, command, reference }, execution);
            // 已受理调用派生读句柄，不重新取得当前 Offer 的执行许可。
            attached = handle;
          }
          // 观察投影失败不改写已经返回的业务结果，更不冒充 tool.settled。
          try {
            const observation = observe(output);
            publishActivity({ state: 'returned', ...(observation === undefined ? {} : { observation }) });
          } catch (observationError) {
            publishActivity({ state: 'returned', observationError });
          }
          return output;
        } catch (error) {
          let failure = error;
          // prepare 可已跨过持久 ACK；只有本框架确定尚未调用 execute 时才能结案。
          // 一旦进入业务函数，异常可能表示未知受理，不能替 owner 猜测 rejected。
          if (prepared && !started) {
            try { await options.operation?.({ phase: 'not-started', ...prepared }, execution); }
            catch (checkpointError) { failure = new AggregateError([error, checkpointError], 'REACTIVE_ACTION_NOT_STARTED_CHECKPOINT_FAILED'); }
          }
          publishActivity({ state: 'failed', phase, error: failure });
          throw failure;
        } finally {
          finished = true;
          if (invocation && active.delete(invocation)) publishActive();
          if (released && !active.size) activeInvocations.complete();
        }
      })();
      // activity$ 也是正式的失败观察入口；仅观察它不制造未处理 Promise 拒绝。
      void result.catch(() => {});
      const operation = operationOwner ? result.then(() => {
        if (!attached) throw new Error('REACTIVE_ACTION_OPERATION_RECEIPT_MISSING');
        return attached;
      }) : undefined;
      void operation?.catch(() => {});
      invocation = Object.freeze({ id: invocationId, source, result, activity, activity$: activity.values$,
        ...(operation ? { operation } : {}) }) as ActionInvocation<Definition>;
      if (!finished) { active.add(invocation); publishActive(); }
      // 所有入口汇入同一 invoke；观察者取得调用方返回的同一个句柄。
      // input 或同步准入可能已经失败，activity$ 保留该次调用的当前及终态。
      invocations.next(invocation);
      return invocation;
    };
    const bound = Object.assign(invoke, {
      definition,
      bindingId,
      scope,
      released: false,
      invoke,
      invocations$: invocations.asObservable(),
      activeInvocations$: activeInvocations.asObservable(),
      observe,
      attach,
      locate,
      release() {
        if (released) return;
        released = true; invocations.complete();
        if (!active.size) activeInvocations.complete();
      },
    });
    Object.defineProperty(bound, 'released', { get: () => released });
    // 此闭包仍捕获具体输入 schema；擦除的 raw 随即进入该 schema 的唯一 parse。
    boundDispatchers.set(bound, {
      operationOwner,
      invoke: (raw, execution) => {
        if (!execution) throw new Error('REACTIVE_ACTION_EXECUTION_REQUIRED');
        return invoke(raw as z.input<I>, execution);
      },
      observe: output => observe(output as z.output<O>),
    });
    return Object.freeze(bound);
}

export function defineAction<I extends z.ZodType, O extends z.ZodType, S extends z.ZodType<Json>, Observed extends Json = never,
  P extends OperationContract = never>(
  options: ActionOptions<I, O, S, Observed, P>,
): ActionDefinition<I, O, S, Observed, P> {
  if (!/^[a-z][a-z0-9_]{0,79}$/u.test(options.name)) throw new Error('REACTIVE_ACTION_NAME_INVALID');
  const version = options.version === undefined ? 1 : options.version;
  if (!Number.isSafeInteger(version) || version < 1) throw new Error('REACTIVE_ACTION_VERSION_INVALID');
  const operation = options.operation ? Object.freeze({ definition: options.operation.definition,
    command: options.operation.command, reference: options.operation.reference }) : undefined;
  const definition: ActionDefinition<I, O, S, Observed, P> = Object.freeze({
    name: options.name,
    version,
    description: options.description,
    scope: options.scope,
    input: options.input,
    output: options.output,
    concurrency: options.concurrency,
    observe: options.observe,
    operation,
  });
  actionDefinitions.add(definition);
  return definition;
}

/** 为同一个纯合同建立宿主实现；创建实现与绑定都不会调用 execute。 */
export function implementAction<I extends z.ZodType, O extends z.ZodType, S extends z.ZodType<Json>, Observed extends Json = never,
  P extends OperationContract = never>(
  definition: ActionDefinition<I, O, S, Observed, P>,
  options: NoInfer<ActionImplementationOptions<ActionDefinition<I, O, S, Observed, P>>>,
): ActionImplementation<ActionDefinition<I, O, S, Observed, P>> {
  if (!actionDefinitions.has(definition)) throw new Error('REACTIVE_ACTION_DEFINITION_INVALID');
  const implementation = Object.freeze({ definition });
  const execute = options.execute;
  const owner = options.operationOwner;
  if (definition.operation) {
    if (!isOperationOwner(owner) || owner.definition !== definition.operation.definition) throw new Error('REACTIVE_ACTION_OPERATION_OWNER_MISMATCH');
  } else if (owner) throw new Error('REACTIVE_ACTION_OPERATION_UNDECLARED');
  const bind: DefinitionBinder<I, O, S, Observed, P> = (scope, admission) =>
    createBinding(definition, scope, admission, execute, owner, implementationArgumentCaptures.get(implementation));
  implementationBindings.set(implementation, bind);
  return implementation;
}

export function bindAction<I extends z.ZodType, O extends z.ZodType, S extends z.ZodType<Json>, Observed extends Json = never,
  P extends OperationContract = never>(
  implementation: ActionImplementation<ActionDefinition<I, O, S, Observed, P>>,
  scope: z.input<NoInfer<S>>,
  options: NoInfer<ActionBindingOptions<ActionDefinition<I, O, S, Observed, P>> & {
    execution(action: ActionReference): ActionExecutionContext;
  }>,
): BoundAction<ActionDefinition<I, O, S, Observed, P>, true>;
export function bindAction<I extends z.ZodType, O extends z.ZodType, S extends z.ZodType<Json>, Observed extends Json = never,
  P extends OperationContract = never>(
  implementation: ActionImplementation<ActionDefinition<I, O, S, Observed, P>>,
  scope: z.input<NoInfer<S>>,
  options: NoInfer<ActionBindingOptions<ActionDefinition<I, O, S, Observed, P>>>,
): BoundAction<ActionDefinition<I, O, S, Observed, P>>;
export function bindAction<I extends z.ZodType, O extends z.ZodType, S extends z.ZodType<Json>, Observed extends Json = never,
  P extends OperationContract = never>(
  implementation: ActionImplementation<ActionDefinition<I, O, S, Observed, P>>,
  scope: z.input<NoInfer<S>>,
  options: NoInfer<ActionBindingOptions<ActionDefinition<I, O, S, Observed, P>>>,
): BoundAction<ActionDefinition<I, O, S, Observed, P>, true> {
  // 只有实际实现对象能取得捕获该合同与 handler 的 binder，公开 definition 字段不是执行凭证。
  const bind = implementationBindings.get(implementation) as DefinitionBinder<I, O, S, Observed, P> | undefined;
  if (!bind) throw new Error('REACTIVE_ACTION_IMPLEMENTATION_INVALID');
  return bind(scope, options);
}

/** schema 或宿主合同下的 JSON 值按原结构复制；冻结自己的快照，不冻结调用者对象。 */
function ownJson<T>(value: T, errorCode: string): ActionTarget<T> {
  return copyJson(value, errorCode) as ActionTarget<T>;
}

function copyJson(value: unknown, errorCode: string, ancestors = new Set<object>()): Json {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return value;
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value !== 'object' || ancestors.has(value)) throw new Error(errorCode);
  ancestors.add(value);
  try {
    if (Array.isArray(value)) {
      const result = Array.from(value, item => copyJson(item, errorCode, ancestors));
      Object.freeze(result);
      return result;
    }
    const prototype: unknown = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) throw new Error(errorCode);
    if (Object.getOwnPropertySymbols(value).length) throw new Error(errorCode);
    const result: Record<string, Json> = Object.fromEntries(Object.entries(value).map(([key, item]) => [key, copyJson(item, errorCode, ancestors)]));
    Object.freeze(result);
    return result;
  } finally {
    ancestors.delete(value);
  }
}
