import type { SlotCore, SlotComponent } from '@deepseek-ai/dsh-client-ui-slots';
import type { ActionContract } from '@hanger-source/dsh-reactive';
import { createNativeActionViews, defineNativeActionView, type NativeActionViewProps, type NativePresentationSource }
  from '@hanger-source/dsh-reactive-native-ui';

/**
 * 由应用选择是否扩展 dsh 原生 Tool 视图。UI 组件由应用提供，后台程序不导入此包。
 * 模块名称与真实工具名跟随会话 manifest；调用结果从原持久 metadata 读取。
 */
export function extendNativeConversation<D extends ActionContract>(
  slots: Pick<SlotCore, 'register'>,
  sessions: readonly NativePresentationSource[],
  action: D,
  render: SlotComponent<NativeActionViewProps<D>>,
  signal: AbortSignal,
) {
  const views = createNativeActionViews(slots, [defineNativeActionView(action, render)]);
  for (const session of sessions) views.attach(session, { signal });
  return views;
}
