import type { SlotComponent, SlotCore, PropsRuntime, InjectFace } from '@deepseek-ai/dsh-client-ui-slots';
import type { SessionBinding, ISessions } from '@deepseek-ai/dsh-api-session-controller/client';
import type { UiConversation, ConversationMatch, ConversationNodeDefinitionInput } from '@deepseek-ai/dsh-client-ui-conversation/client';
import type { ChatNode } from '@deepseek-ai/dsh-client-ui-chat/client';
import type { RemoteProgramSession } from '@hanger-source/dsh-reactive-client';
import { readActionPresentation, type ActionPresentation, type ActionInput, type ProgramInput, type Submission,
  type HumanQuestion, type HumanForegroundQuestion, type HumanAnswer, type ResourceReadable, type z } from '@hanger-source/dsh-reactive';
import { bindNativeSession, nativeSessionInject, registerNativeConversation,
  type NativeSession } from '@hanger-source/dsh-reactive-native-ui';
import { InventoryProgram, RequestReplenishment, stockSchema, inventoryTarget } from './inventory-contract.ts';

type Receipt = ActionPresentation<typeof RequestReplenishment>;
declare module '@deepseek-ai/dsh-client-ui-chat/client' {
  interface ChatNodeDataMap { 'inventory-replenishment': Receipt; }
}

/** 真实 tool/result 才产生业务回执行；历史重放仍由 UiConversation 解释。 */
function receiptAt(match: ConversationMatch): Receipt {
  if (match.event.type !== 'tool/result') throw new Error('INVENTORY_RECEIPT_EVENT_REQUIRED');
  const receipt = readActionPresentation(RequestReplenishment, match.event.data.meta);
  if (!receipt) throw new Error('INVENTORY_RECEIPT_REQUIRED');
  return receipt;
}
const receiptNode: ConversationNodeDefinitionInput<Receipt> = {
  kind: 'inventory-replenishment', target: 'chat',
  match: { 'tool/result': event => {
    if (event.type !== 'tool/result' || !readActionPresentation(RequestReplenishment, event.data.meta)) return null;
    return { id: event.data.message.toolCallId, role: 'start' };
  } },
  start: (_context, match) => receiptAt(match),
  update: (_context, match) => receiptAt(match),
  buildViewNode: context => {
    if (!context.state || !context.start) return null;
    return { key: context.key, id: context.id, kind: 'inventory-replenishment', target: 'chat',
      anchorSeq: context.start.event.seq, location: context.start.location, visibility: 'visible', data: context.state };
  },
};

/** SDK 和原资源仍由调用方持有；这份 UI 连接只拥有观察与展示贡献。 */
export function connectInventorySession(native: SessionBinding, sdk: RemoteProgramSession<typeof InventoryProgram>,
  stock: ResourceReadable<z.output<typeof stockSchema>>, target: z.output<typeof inventoryTarget>) {
  const session = bindNativeSession(InventoryProgram, native, sdk);
  const model = {
    session,
    stock: session.resource(stock),
    questions: session.questions,
    foreground: session.foregroundQuestions,
    async replenish(input: ActionInput<typeof RequestReplenishment>) {
      const action = await session.resolveAction('replenish', target);
      const invocation = await action(input);
      // 观察与命令分开；这两条读侧没有再次调用 Action，也不取消正式补货。
      return { invocation, activity: session.invocation(invocation),
        operation: session.operation(await invocation.operation) };
    },
    submit(input: Submission<ProgramInput<typeof InventoryProgram>>) { return session.submit(input); },
    answer(question: HumanQuestion | HumanForegroundQuestion, answer: HumanAnswer) { return session.answer(question, answer); },
    claim(question: HumanQuestion | HumanForegroundQuestion) { return session.claim(question); },
  };
  return model;
}
export type InventorySession = ReturnType<typeof connectInventorySession>;

function inventoryInjection(sessions: Pick<ISessions, 'binding'>, models: readonly InventorySession[]) {
  return nativeSessionInject(sessions, {
    resolve: native => models.find(model => model.session.native === native)?.session,
    sources: {
      stock: session => {
        const model = models.find(model => model.session === session);
        if (!model) throw new Error('INVENTORY_UI_SESSION_UNBOUND');
        return model.stock;
      },
      questions: session => session.questions,
      foreground: session => session.foregroundQuestions,
    },
  });
}
export type InventoryPanelProps = PropsRuntime<'conversation.input.dock'>
  & InjectFace<ReturnType<ReturnType<typeof inventoryInjection>>>;

/**
 * 原生组件由应用提供；框架负责类型化接线和退出。
 * Panel 使用 useStock/useQuestions/useForeground 读 pending/error/ready，参与通过其 program。
 * 注册本身不问人、不提交输入、不补货，也不连接默认 Controller 的 prompt/create。
 */
export function installInventoryConversation(
  slots: Pick<SlotCore, 'register' | 'registerFactory'>,
  conversation: Pick<UiConversation, 'events' | 'views' | 'groups' | 'userText'>,
  sessions: Pick<ISessions, 'binding'>,
  models: readonly InventorySession[],
  components: {
    readonly Panel: SlotComponent<InventoryPanelProps>;
    readonly Receipt: SlotComponent<PropsRuntime<'conversation.chat.node', 'inventory-replenishment'>>;
  },
) {
  const inject = inventoryInjection(sessions, models);
  return registerNativeConversation(conversation, slots, {
    events: [receiptNode],
    render: registry => {
      const receipt = registry.register({ name: 'conversation.chat.node', key: 'inventory-replenishment' }, components.Receipt);
      try {
        const panel = registry.register({ name: 'conversation.input.dock', id: 'inventory', inject }, components.Panel);
        return () => { panel(); receipt(); };
      } catch (error) { receipt(); throw error; }
    },
  });
}

/** 应用退出先撤整组展示，再撤 UI 读侧；原 SDK / native reference 仍单独释放。 */
export function disconnectInventory(releaseViews: () => void, models: readonly InventorySession[]) {
  try { releaseViews(); }
  finally { for (const model of models) model.session.dispose(); }
}
