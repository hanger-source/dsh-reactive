import { homedir } from 'node:os';
import type { Context } from '@deepseek-ai/cordis';
import type { TypertRemoteEventDispatch, TypertRemoteEventSource } from '@deepseek-ai/dsh-api-gateway';
import { API_REMOTE_FORWARDED_EVENTS } from '@deepseek-ai/dsh-api-remotes';
import { z } from 'zod';
import { Inbox } from '@hanger-source/dsh-reactive-client/transport';

/** 只转发真实 Controller 通知；人工 waterfall 由 Program 的 HumanInput owner 接收。 */
export function registerProgramEvents(ctx: Context): () => Promise<void> {
  const source: TypertRemoteEventSource = signal => {
    const inbox = new Inbox<TypertRemoteEventDispatch>(1024 * 1024);
    const stops = API_REMOTE_FORWARDED_EVENTS.filter((entry): entry is Extract<typeof API_REMOTE_FORWARDED_EVENTS[number],
      { readonly event: `api-session/${string}`; readonly mode: 'emit' }> => entry.mode === 'emit' && entry.event.startsWith('api-session/'))
      .map(entry => ctx.on(entry.event, (...args: unknown[]) => {
        try { inbox.push({ event: entry.event, args: z.array(z.json()).parse(args),
          ...('resources' in entry ? { resources: entry.resources } : {}) }); }
        catch (error) { inbox.fail(error); }
      }));
    const abort = (): void => { stops.forEach(stop => stop()); inbox.end(); };
    signal.addEventListener('abort', abort, { once: true });
    if (signal.aborted) abort();
    // source 返回前已经登记所有 listener；Gateway 才能在此后发布 generation ready。
    return (async function* () {
      try { yield* inbox; }
      finally { signal.removeEventListener('abort', abort); abort(); }
    })();
  };
  return ctx.typertGateway.registerRemoteEvents(source, { home: homedir() });
}
