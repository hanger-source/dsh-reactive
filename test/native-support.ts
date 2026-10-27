import { LlmAdapter, ToolCallId, type GenerateOptions, type StreamChunk } from '@deepseek-ai/dsh-llm';
import type { Context } from '@deepseek-ai/cordis';
import { mkdir } from 'node:fs/promises';
import { join } from 'node:path';
import { createRuntime, nativeModel } from '@hanger-source/dsh-reactive-dsh';
import type { ApplicationDefinition, AnyEnvironment } from '@hanger-source/dsh-reactive';

export async function boot<E extends readonly AnyEnvironment[]>(root: string, definition: ApplicationDefinition<E>,
  respond: (options: GenerateOptions) => AsyncIterable<StreamChunk>, tools: 'native' | 'ptc' = 'native',
  workspaceDirectory: string = join(root, 'workspace')) {
  const errors: unknown[] = [];
  let native!: Context;
  const model = nativeModel({ provider: 'isolated', model: 'test', create: () => new class extends LlmAdapter {
    stream(options: GenerateOptions) { return respond(options); }
  }() });
  const runtime = await createRuntime(() => definition, { home: root, actorId: 'operator-test', serviceActorId: 'program-service', tools, goalRounds: 3,
    onError: event => { errors.push(event.error); },
    model: { ...model, async install(ctx, home) { native = ctx; await model.install(ctx, home); } } });
  try {
    await mkdir(workspaceDirectory, { recursive: true });
    const workspace = await runtime.application.createWorkspace({ directory: workspaceDirectory });
    return { host: runtime.application, workspace, native, errors, dispose: () => runtime.dispose() };
  } catch (error) {
    try { await runtime.dispose(); }
    catch (cleanup) { throw new AggregateError([error, cleanup], 'TEST_WORKSPACE_CREATION_FAILED'); }
    throw error;
  }
}
export async function* tool(name: string, id: string, args: unknown): AsyncIterable<StreamChunk> {
  yield { type: 'block-start', index: 0, blockType: 'tool-call' };
  yield { type: 'tool-call-delta', index: 0, id: ToolCallId(id), name, argumentsDelta: JSON.stringify(args) };
  yield { type: 'block-end', index: 0, block: { type: 'tool-call', id: ToolCallId(id), name, arguments: JSON.stringify(args) } };
  yield { type: 'finish', reason: { kind: 'tool-calls' } };
}
export async function* text(value: string): AsyncIterable<StreamChunk> {
  yield { type: 'block-start', index: 0, blockType: 'text' };
  yield { type: 'text-delta', index: 0, text: value };
  yield { type: 'block-end', index: 0, block: { type: 'text', text: value } };
  yield { type: 'finish', reason: { kind: 'stop' } };
}
