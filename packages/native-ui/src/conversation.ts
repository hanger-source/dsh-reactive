import type { SlotCore } from '@deepseek-ai/dsh-client-ui-slots';
import type { UiConversation, ConversationNodeDefinitionInput, ConversationViewDefinition, ConversationViewNode,
  ConversationGroupDefinition, ConversationGroupData, UserTextRule } from '@deepseek-ai/dsh-client-ui-conversation/client';
import { installNativeContributions } from './slots.ts';

export interface NativeConversationContribution<State, Node extends ConversationViewNode, Snapshot, Target extends string, GroupState> {
  readonly events?: readonly ConversationNodeDefinitionInput<State>[];
  readonly view?: ConversationViewDefinition<Node, Snapshot> & { readonly target: Target };
  readonly groups?: ConversationGroupDefinition<Node, GroupState, ConversationGroupData<Target>> & { readonly target: Target };
  readonly userText?: readonly UserTextRule[];
  /** 直接使用原生 typed register；不复制 renderer props、children、store 或 factory 合同。 */
  readonly render?: (slots: Pick<SlotCore, 'register' | 'registerFactory'>) => () => void;
}

/**
 * 同一份事件解释、target/group 及 renderer 贡献整组进入/退出。
 * 原生 UiConversation 仍拥有历史重放、节点/分组状态、异步 publication 和 Session builder。
 */
export function registerNativeConversation<State, Node extends ConversationViewNode = ConversationViewNode,
  Snapshot = unknown, const Target extends string = string, GroupState = unknown>(
  conversation: Pick<UiConversation, 'events' | 'views' | 'groups' | 'userText'>,
  slots: Pick<SlotCore, 'register' | 'registerFactory'>,
  contribution: NativeConversationContribution<State, Node, Snapshot, Target, GroupState>,
): () => void {
  if (contribution.view && contribution.groups && contribution.view.target !== contribution.groups.target)
    throw new Error('REACTIVE_NATIVE_CONVERSATION_TARGET_MISMATCH');
  const installers: (() => () => void)[] = [];
  for (const event of contribution.events ?? []) installers.push(() => conversation.events.register(event));
  if (contribution.view) installers.push(() => conversation.views.register(contribution.view!));
  if (contribution.groups) installers.push(() => conversation.groups.register(contribution.groups!));
  for (const rule of contribution.userText ?? []) installers.push(() => conversation.userText.register(rule));
  if (contribution.render) installers.push(() => contribution.render!(slots));
  return installNativeContributions(installers);
}
