import { defineAction, defineProgram, defineSurface, z } from '@hanger-source/dsh-reactive';
import type { RemoteProgramSession } from '@hanger-source/dsh-reactive-client';
import { createNativeActionViews, defineNativeActionView, bindNativeSession,
  type NativeActionViewProps } from '@hanger-source/dsh-reactive-native-ui';
import { getDeveloperSkill } from '@hanger-source/dsh-reactive-skills';

const action = defineAction({ name: 'release_consumer_read', description: '公开 UI 类型消费',
  scope: z.object({ subject: z.string() }).strict(), input: z.null(), output: z.object({ version: z.string() }).strict() });
const program = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: { read: action } }) });
export type Session = RemoteProgramSession<typeof program>;
export const view = defineNativeActionView(action, (_props: NativeActionViewProps<typeof action>) => null);
export const publicEntries = [createNativeActionViews, bindNativeSession, getDeveloperSkill] as const;
