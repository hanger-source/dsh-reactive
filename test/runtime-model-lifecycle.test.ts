import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdir, mkdtemp, readFile } from 'node:fs/promises';
import { once } from 'node:events';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { CONTEXT_WINDOW_EXCEEDED_CODE, IMAGE_OFFLOAD_REQUIRED_CODE, LlmAdapter, LlmError, resolveRetryPolicy, type GenerateOptions, type LlmResolvedModelInfo, type StreamChunk } from '@deepseek-ai/dsh-llm';
import { SessionId } from '@deepseek-ai/dsh-session';
import type {} from '@deepseek-ai/dsh-llm-retry';
import type {} from '@deepseek-ai/dsh-compaction';
import type {} from '@deepseek-ai/dsh-compaction-image-offload/projection';
import type {} from '@deepseek-ai/dsh-token-meter';
import { defineAction, defineApplication, defineEnvironment, defineProgram, defineSurface, implementAction, z } from '@hanger-source/dsh-reactive';
import { implementProgram, Offer, useAction } from '@hanger-source/dsh-reactive/program';
import { createRuntime, deepseek, nativeModel } from '@hanger-source/dsh-reactive-dsh';
import { text, tool } from './native-support.ts';

const gif = 'R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7';

test('官方模型恢复链重试请求、压缩历史并卸载图片，取消退避不重执业务，冷恢复重放同一持久事实', { timeout: 30_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-model-lifecycle-'));
  const effects: string[] = [], errors: unknown[] = [];
  const requests: GenerateOptions[] = [], summaries: GenerateOptions[] = [];
  const cancellations = Promise.withResolvers<void>();
  const compactions: Array<{ id: string; before: number; after?: number }> = [];
  const stageCalls = new Map<string, number>();
  let stage: 'record' | 'pressure' | 'image' | 'overflow' | 'cancel' = 'record';
  let totalCalls = 0;
  const record = defineAction({ name: 'record_delivery_details', description: '记录本次已提供的商家交付资料',
    scope: z.object({ merchantId: z.string() }).strict(), input: z.object({ revision: z.literal(7) }).strict(),
    output: z.object({ recorded: z.string(), revision: z.literal(7) }).strict() });
  const implementation = implementAction(record, { execute(input, call) {
    effects.push(call.scope.merchantId);
    return { recorded: call.scope.merchantId, revision: input.revision };
  } });
  const program = defineProgram({ input: z.object({ merchantId: z.string() }).strict(), surface: defineSurface({ views: {}, actions: {} }) });
  const definition = defineApplication({ name: 'model-lifecycle', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '核对商家资料，记录工具结果；历史摘要不是新的记录要求。',
    program: implementProgram(program, { Component() {
      const action = useAction(implementation, () => ({ merchantId: 'merchant-a' }), []);
      return Offer({ name: record.name, action });
    } }),
  })] });
  const model = nativeModel({ provider: 'lifecycle', model: 'bounded', create: () => new class extends LlmAdapter {
    override resolveModel(provider: string, id: string, signal?: AbortSignal): Promise<LlmResolvedModelInfo> {
      signal?.throwIfAborted();
      // 容量属于这个真实适配器的 exact route，由官方请求头与压缩器读取。
      return Promise.resolve({ provider, id, name: '有界试验模型', context: { contextWindow: 8_192 }, defaultMaxTokens: 256,
        inputModalities: ['text', 'image'] });
    }
    override providerRetryPolicy() {
      return resolveRetryPolicy({ mode: 'normal', maxRetries: 1, retryableCodes: ['RATE_LIMIT'],
        backoff: { initialDelayMs: 1, maxDelayMs: 30_000, jitterRatio: 0 } }, 'lifecycle.retryPolicy');
    }
    async *stream(options: GenerateOptions): AsyncIterable<StreamChunk> {
      if (++totalCalls > 32) throw new Error('MODEL_LIFECYCLE_REQUEST_LIMIT');
      if (options.purpose === 'compaction') {
        summaries.push(options);
        yield* text('Merchant a delivery details revision 7 were recorded once. Retain the recorded receipt; later messages only add reference material.');
        return;
      }
      requests.push(options);
      const count = (stageCalls.get(stage) ?? 0) + 1;
      stageCalls.set(stage, count);
      if (stage === 'record') {
        if (count === 1) throw new LlmError('暂时限流，重试原模型请求', 'RATE_LIMIT');
        if (count === 2) { yield* tool(record.name, 'record-once', { revision: 7 }); return; }
        yield* text('Merchant a delivery details revision 7 were recorded.');
        return;
      }
      if (stage === 'image') {
        const images = options.messages.flatMap(message => message.content.filter(block => block.type === 'image'));
        assert.equal(images.length, 1, '同一次已准入图片输入在重试时保持原 occurrence');
        if (count === 1) {
          assert.equal(images[0]!.offloaded, undefined);
          throw new LlmError('该路由需要先卸载最旧图片 occurrence', IMAGE_OFFLOAD_REQUIRED_CODE, { offloadImages: 1 });
        }
        assert.equal(images[0]!.offloaded, true);
        yield* text('The image reference remains available; the request now omits its image bytes.');
        return;
      }
      if (stage === 'overflow' && count === 1) throw new LlmError('供应方报告上下文溢出', CONTEXT_WINDOW_EXCEEDED_CODE);
      if (stage === 'cancel') throw new LlmError('等待供应方明确的退避时间', 'RATE_LIMIT', { providerRetryAfterMs: 30_000 });
      yield* text('Reference material was read without repeating the recorded delivery action.');
    }
  }() });
  const runtime = await createRuntime(() => definition, { home: join(root, 'home'), actorId: 'merchant-operator', tools: 'native', model,
    compaction: { thresholdRatio: 0.5, headroomTokens: 512, retainTokens: 128, maxTokens: 128, compactionRetries: 0 },
    onError: event => errors.push(event.error) });
  const stop = runtime.native.on('session/event', (session, event) => {
    if (event.type === 'compaction/start') compactions.push({ id: event.data.compactionId,
      before: runtime.native.tokenMeter.measure(session).surfaceTokens });
    if (event.type === 'compaction/end') {
      const entry = compactions.find(value => value.id === event.data.compactionId);
      if (entry) entry.after = runtime.native.tokenMeter.measure(session).surfaceTokens;
    }
    if (stage === 'cancel' && event.type === 'llm/retry') cancellations.resolve();
  });
  try {
    const directory = join(root, 'workspace'); await mkdir(directory);
    const workspace = await runtime.application.createWorkspace({ directory });
    const session = await runtime.application.create('work', { workspaceId: workspace.workspaceId });
    const send = async (requestId: string, content: string) => {
      await session.submit({ requestId, content, scope: { merchantId: 'merchant-a' }, intent: 'question', delivery: 'followup' });
      await session.whenIdle();
    };
    await send('record', '请记录商家甲第七版交付资料。');
    const agent = runtime.native.agents.get(SessionId(session.id))!;
    assert.deepEqual(effects, ['merchant-a']);
    assert.equal(stageCalls.get('record'), 3, '重试失败模型请求后执行一次工具，再读取一次真实工具结果');
    let events = agent.session.snapshotEvents();
    const retries = events.filter(event => event.type === 'llm/retry');
    assert.equal(retries.length, 1);
    const retried = retries[0]!;
    assert.equal(retried.data.failure.code, 'RATE_LIMIT');
    assert.ok(events.some(event => event.type === 'llm/retry-started' && event.data.retryId === retried.data.retryId));
    const calls = runtime.native.sessionProjections.stateOf(agent.session, 'reactiveInputs')!.calls;
    assert.equal(calls['record-once']?.inputs[0]?.requestId, 'record');
    assert.equal(calls['record-once']?.inputs[0]?.actorId, 'merchant-operator');

    stage = 'pressure';
    const material = 'Supplier itinerary revision departure weekdays contact channel delivery facts. '.repeat(80);
    for (let index = 1; index <= 6 && summaries.length === 0; index++) await send('pressure-' + index, `Reference batch ${index}. ${material}`);
    assert.ok(summaries.length > 0, '累积的真实会话压力必须触发自动摘要，不能靠手动compactNow代替装配');
    assert.ok(compactions.some(value => value.after !== undefined && value.after < value.before), '持久surface替换后官方meter反映实际减少');
    events = agent.session.snapshotEvents();
    const pressureSummary = events.find(event => event.type === 'compaction/summary');
    assert.ok(pressureSummary?.type === 'compaction/summary');
    assert.ok(pressureSummary.data.shadowedSeqs.length > 0);
    assert.equal(pressureSummary.data.llmStreamCall, true);
    assert.ok(events.some(event => event.type === 'user/message' && event.data.source.kind === 'compact-checkpoint'));
    assert.deepEqual(effects, ['merchant-a'], '摘要和模型重试均不重复业务副作用');

    stage = 'image';
    await session.submit({ requestId: 'image', content: [
      { type: 'text', text: '请核对图片资料，记录行为已经完成。' }, { type: 'image', mediaType: 'image/gif', data: gif, name: 'delivery.gif' },
    ], scope: { merchantId: 'merchant-a' }, intent: 'question', delivery: 'followup' });
    await session.whenIdle();
    assert.equal(stageCalls.get('image'), 2, 'image-offload按真实持久进展重试，不依赖普通RATE_LIMIT策略');
    events = agent.session.snapshotEvents();
    const imageInput = events.find(event => event.type === 'user/message' && 'rpcId' in event.data.source && event.data.source.rpcId === 'image');
    assert.ok(imageInput?.type === 'user/message');
    const originalImage = imageInput.data.content.find(block => block.type === 'image');
    assert.ok(originalImage?.type === 'image');
    assert.equal(originalImage.offloaded, undefined, '卸载是投影决策，不篡改原输入或删除附件');
    await runtime.native.attachments.readImage(originalImage.attachment);
    assert.ok(events.some(event => event.type === 'image/offload' && event.data.targets.some(target =>
      target.seq === imageInput.seq && target.imageIndexes.includes(0))));

    stage = 'overflow';
    const summaryCount = summaries.length;
    await send('overflow', 'Continue with this retained reference material. ' + material.slice(0, 1_500));
    assert.equal(stageCalls.get('overflow'), 2);
    assert.ok(summaries.length > summaryCount, 'canonical overflow沿compaction恢复，不冒用普通请求重试codes');
    assert.deepEqual(effects, ['merchant-a']);
    assert.deepEqual(errors, []);

    stage = 'cancel';
    await session.submit({ requestId: 'cancel-backoff', content: '取消等待中的模型请求，不取消已记录业务。',
      scope: { merchantId: 'merchant-a' }, intent: 'question', delivery: 'followup' });
    await cancellations.promise;
    await session.cancel(); await session.whenIdle();
    assert.equal(stageCalls.get('cancel'), 1, '取消退避必须阻止第二次provider调用');
    events = agent.session.snapshotEvents();
    const cancelled = events.filter(event => event.type === 'llm/retry').at(-1)!;
    assert.ok(!events.some(event => event.type === 'llm/retry-started' && event.data.retryId === cancelled.data.retryId));
    assert.deepEqual(effects, ['merchant-a']);
    const pressure = runtime.native.tokenMeter.measure(agent.session);
    const originalRequest = session.snapshot().requests.record;
    const callsBeforeColdRead = totalCalls;
    const originalId = session.id;
    session.release(); await runtime.application.close(originalId);
    const cold = await runtime.application.open(originalId, program);
    assert.equal(runtime.native.agents.get(SessionId(originalId)), undefined);
    assert.deepEqual(cold.snapshot().requests.record, originalRequest);
    const resumed = await runtime.application.resume(originalId, program);
    const restored = runtime.native.agents.get(SessionId(originalId))!;
    assert.notEqual(restored.session, agent.session);
    assert.equal(totalCalls, callsBeforeColdRead, '冷读取与空闲恢复只重放事实，不重新请求模型或执行业务');
    assert.equal(runtime.native.tokenMeter.measure(restored.session).surfaceTokens, pressure.surfaceTokens);
    assert.equal(runtime.native.sessionProjections.snapshot(restored.session, ['contextPressure']).values.contextPressure?.contextWindow, 8_192);
    assert.ok(pressureSummary.data.shadowedSeqs.every(seq => !restored.session.surface.nodes.includes(seq)));
    assert.ok(restored.session.snapshotEvents().some(event => event.type === 'image/offload' && event.data.targets.some(target => target.seq === imageInput.seq)));
    await runtime.native.attachments.readImage(originalImage.attachment);
    assert.equal(runtime.native.sessionProjections.stateOf(restored.session, 'reactiveInputs')!.calls['record-once']?.inputs[0]?.requestId, 'record');
    assert.deepEqual(effects, ['merchant-a']);
    cold.release(); resumed.release();
  } finally { stop(); await runtime.dispose(); }
});

test('正式DeepSeek适配器传送已准入图片、按路由限额卸载旧occurrence并把上传复用索引保存在独立home', { timeout: 15_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-deepseek-images-'));
  const home = join(root, 'home');
  const failures: unknown[] = [], uploaded: File[] = [];
  const wireSchema = z.object({ messages: z.object({ content: z.object({ type: z.string() }).passthrough().array() }).passthrough().array() }).passthrough();
  const messages: z.output<typeof wireSchema>[] = [];
  const credential = 'isolated-adapter-test-key';
  let httpRequests = 0;
  const server = createServer((request, response) => {
    void (async () => {
      if (++httpRequests > 8) throw new Error('DEEPSEEK_HTTP_REQUEST_LIMIT');
      assert.equal(request.method, 'POST');
      assert.equal(request.headers.authorization, 'Bearer ' + credential);
      const chunks: Buffer[] = [];
      for await (const chunk of request) chunks.push(Buffer.from(chunk));
      const body = Buffer.concat(chunks);
      if (request.url === '/v1/files') {
        const form = await new Response(body, { headers: { 'content-type': String(request.headers['content-type']) } }).formData();
        const file = form.get('file');
        assert.ok(file instanceof File);
        assert.ok(file.size > 0);
        uploaded.push(file);
        response.writeHead(200, { 'content-type': 'application/json' });
        response.end(JSON.stringify({ id: 'isolated-image-file', type: 'file', mime_type: file.type, size_bytes: file.size,
          filename: file.name, created_at: new Date().toISOString() }));
        return;
      }
      assert.equal(request.url, '/v1/messages');
      messages.push(wireSchema.parse(JSON.parse(body.toString('utf8'))));
      assert.ok(messages.length <= 2, '限额恢复不重复已经完成的HTTP模型请求');
      response.writeHead(200, { 'content-type': 'text/event-stream' });
      const events = [
        { type: 'message_start', message: { id: 'message-' + messages.length, role: 'assistant', content: [], usage: { input_tokens: 48, output_tokens: 0 } } },
        { type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' } },
        { type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: '已核对当前图片引用。' } },
        { type: 'content_block_stop', index: 0 },
        { type: 'message_delta', delta: { stop_reason: 'end_turn' }, usage: { output_tokens: 8 } },
        { type: 'message_stop' },
      ];
      response.end(events.map(event => `event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`).join(''));
    })().catch(error => {
      failures.push(error);
      response.writeHead(500, { 'content-type': 'application/json' });
      response.end(JSON.stringify({ error: { type: 'test_transport_failure', message: '检查HTTP边界失败' } }));
    });
  });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  try {
    const address = server.address(); assert.ok(address && typeof address !== 'string');
    const model = deepseek({ model: 'deepseek-flash', apiKey: credential, baseURL: `http://127.0.0.1:${address.port}`, thinking: 'disabled',
      maxTokens: 256, maxImagesPerRequest: 1, imageOffloadCountQuantum: 1, retryPolicy: { mode: 'normal', maxRetries: 0 } });
    const definition = defineApplication({ name: 'deepseek-image-wire', admit: async () => {}, environments: [defineEnvironment({
      id: 'work', instructions: '核对本次商家图片，之前图片的只读引用仍可用于查阅。',
      program: implementProgram(defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) }), { Component: () => null }),
    })] });
    const runtime = await createRuntime(() => definition, { home, actorId: 'merchant-operator', tools: 'native', model,
      onError: event => failures.push(event.error) });
    try {
      const directory = join(root, 'workspace'); await mkdir(directory);
      const workspace = await runtime.application.createWorkspace({ directory });
      const session = await runtime.application.create('work', { workspaceId: workspace.workspaceId });
      for (const name of ['first-delivery', 'current-delivery']) {
        await session.submit({ requestId: name, content: [
          { type: 'text', text: '核对 ' + name }, { type: 'image', mediaType: 'image/gif', data: gif, name: name + '.gif' },
        ], scope: null, intent: 'question', delivery: 'followup' });
        await session.whenIdle();
      }
      assert.deepEqual(failures, []);
      assert.equal(messages.length, 2);
      const imageWire = z.object({ source: z.object({ type: z.literal('file'), file_id: z.string() }) });
      for (const message of messages) {
        const images = message.messages.flatMap(value => value.content.filter(block => block.type === 'image'));
        assert.equal(images.length, 1, '实际HTTP正文只保留路由允许的一个图片occurrence');
        assert.equal(imageWire.parse(images[0]).source.file_id, 'isolated-image-file');
      }
      assert.equal(uploaded.length, 1, '同一准入附件的相同请求版本使用正式Files索引，不重复上传');
      const agent = runtime.native.agents.get(SessionId(session.id))!;
      const events = agent.session.snapshotEvents();
      const original = events.find(event => event.type === 'user/message' && 'rpcId' in event.data.source && event.data.source.rpcId === 'first-delivery');
      assert.ok(original?.type === 'user/message');
      const image = original.data.content.find(block => block.type === 'image'); assert.ok(image?.type === 'image');
      assert.ok(events.some(event => event.type === 'image/offload' && event.data.targets.some(target => target.seq === original.seq)));
      const secondText = messages[1]!.messages.flatMap(message => message.content).filter(block => block.type === 'text')
        .map(block => z.object({ text: z.string() }).parse(block).text).join('\n');
      assert.ok(secondText.includes(image.attachment.attachmentId), '卸载后的HTTP正文保留原附件的可恢复身份');
      const path = runtime.native.attachments.imageHostPath(image.attachment); assert.ok(path);
      assert.ok(secondText.includes(path), '卸载后的HTTP正文携当前fs提供的只读访问路径');
      await runtime.native.attachments.readImage(image.attachment);
      const index = await readFile(join(home, 'llm-deepseek', 'files-v3.json'), 'utf8');
      assert.ok(index.includes('isolated-image-file'));
      assert.ok(!index.includes(credential), '上传复用索引不保存请求凭据');
      session.release();
    } finally { await runtime.dispose(); }
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
  }
});
