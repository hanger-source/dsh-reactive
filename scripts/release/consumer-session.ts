import type { ActionInput, CancellationOptions, ProgramContract, ProgramSessionClient, Surface, SurfaceClient, ViewValue, z } from '@hanger-source/dsh-reactive';

// 同一模块仅依赖共享合同，本地、远程和浏览器消费者直接传入原对象。
export function sessionResources<P extends ProgramContract>(session: ProgramSessionClient<P>) {
  return { surface: session.surface, progress: session.progress, human: session.human,
    request: (id: string) => session.request(id), question: (callId: string) => session.question(callId) };
}

export async function readSessionView<P extends ProgramContract, K extends keyof P['surface']['views'] & string>(
  session: ProgramSessionClient<P>, name: K, options?: CancellationOptions,
): Promise<ViewValue<P['surface']['views'][K]>> {
  return session.readView(name, options);
}

export async function callPublishedAction<S extends Surface, K extends keyof S['actions'] & string>(
  client: SurfaceClient<S>, name: K, scope: z.input<S['actions'][K]['scope']>, input: ActionInput<S['actions'][K]>, options?: CancellationOptions,
) {
  const binding = await client.resolveAction(name, scope, options);
  return await binding(input, options);
}
