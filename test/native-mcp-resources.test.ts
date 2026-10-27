import assert from 'node:assert/strict';
import { test } from 'node:test';
import { once } from 'node:events';
import { createServer } from 'node:http';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { useLayoutEffect } from 'react';
import { ReplaySubject, filter, firstValueFrom, timeout } from 'rxjs';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { LlmAdapter } from '@deepseek-ai/dsh-llm';
import { SessionId } from '@deepseek-ai/dsh-session';
import { combineResources, defineApplication, defineEnvironment, defineProgram, defineSurface, z,
  type Observation } from '@hanger-source/dsh-reactive';
import { implementProgram, NativeTools, useResource } from '@hanger-source/dsh-reactive/program';
import { createRuntime, nativeModel } from '@hanger-source/dsh-reactive-dsh';
import { createNativeMcp } from '@hanger-source/dsh-reactive-dsh/mcp';
import { createNativeJobs } from '@hanger-source/dsh-reactive-dsh/jobs';

for (const resources of ['disabled', 'install'] as const) {
  test(`MCP ${resources} 沿真实HTTP连接同步工具资源，同源hooks与Jobs组合不执行模型或工具`, { timeout: 20_000 }, async () => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-mcp-resource-'));
    const errors: unknown[] = [];
    let models = 0, executions = 0;
    const server = new McpServer({ name: 'inventory', version: '1.0.0' });
    const stock = server.registerTool('Stock-Count', { description: '库存查询', inputSchema: {} }, async () => {
      executions++; return { content: [{ type: 'text', text: '10' }] };
    });
    server.registerResource('inventory', 'inventory://current', {}, async uri => ({
      contents: [{ uri: uri.href, text: 'actual-resource-body' }],
    }));
    const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: () => crypto.randomUUID() });
    await server.connect(transport);
    const http = createServer((request, response) => {
      void transport.handleRequest(request, response).catch(error => {
        errors.push(error); response.destroy(error);
      });
    });
    http.listen(0, '127.0.0.1'); await once(http, 'listening');
    const address = http.address();
    assert.ok(address && typeof address !== 'string');
    const mcp = createNativeMcp({ resources, connection: {
      transport: 'streamable-http', serverName: 'Inventory-North', url: `http://127.0.0.1:${address.port}/mcp`,
      headers: {}, toolCallTimeoutMs: 3_000, failOnStartupError: true, reconnect: { enabled: false },
    } });
    const jobs = createNativeJobs({ registry: { kind: 'local' }, tools: { completionDelivery: 'quiet' } });
    assert.equal(mcp.toolNames.peek().status, 'pending', '构造资源不连接原生服务');
    const programNames = new ReplaySubject<readonly string[]>(1);
    const definition = defineApplication({ name: 'mcp-resource-' + resources, admit: async () => {}, environments: [defineEnvironment({
      id: 'work', instructions: '只有当前已注册工具参与程序',
      program: implementProgram(defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) }), {
        Component() {
          const names = useResource(mcp.toolNames), jobNames = useResource(jobs.toolNames), resourceNames = useResource(mcp.resourceToolNames);
          useLayoutEffect(() => {
            if (names.state.status === 'ready') programNames.next(names.state.value);
          }, [names.state]);
          return NativeTools({ names: [names.state, jobNames.state, resourceNames.state].flatMap(
            state => state.status === 'ready' ? state.value : []) });
        },
      }),
    })] });
    let runtime: Awaited<ReturnType<typeof createRuntime>> | undefined;
    const snapshots = new ReplaySubject<Observation<{ mcp: readonly string[]; jobs: readonly string[]; resources: readonly string[] }>>(1);
    let subscription: ReturnType<typeof snapshots.subscribe> | undefined;
    const namesContaining = (name: string) => firstValueFrom(mcp.toolNames.values$.pipe(filter(names => names.includes(name)), timeout(3_000)));
    try {
      runtime = await createRuntime(() => definition, { home: join(root, 'home'), actorId: 'mcp-resources',
        onError: event => errors.push(event.error),
        async installNative(ctx, home) { await jobs.install(ctx, home); await mcp.install(ctx, home); },
        model: nativeModel({ provider: 'mcp-resources', model: 'unused', create: () => new class extends LlmAdapter {
          async *stream(): AsyncGenerator<never> { models++; throw new Error('MCP_RESOURCE_MUST_NOT_REQUEST_MODEL'); }
        }() }),
      });
      subscription = combineResources({ mcp: mcp.toolNames.observations$, jobs: jobs.toolNames.observations$,
        resources: mcp.resourceToolNames.observations$ }).subscribe(snapshots);
      const combined = await firstValueFrom(snapshots.pipe(filter(state => state.status === 'ready'), timeout(3_000)));
      assert.equal(combined.status, 'ready');
      assert.ok(combined.value.mcp.includes('mcp__Inventory-North__Stock-Count'));
      assert.ok(combined.value.jobs.includes('job_output'));
      assert.equal(combined.value.resources.includes('read_mcp_resource'), resources === 'install');
      assert.equal(mcp.toolNames$, mcp.toolNames.values$);
      assert.equal(mcp.resourceToolNames$, mcp.resourceToolNames.values$);
      assert.ok(Object.isFrozen(await mcp.toolNames.read()));

      const workspace = await runtime.application.createWorkspace({ directory: root });
      const session = await runtime.application.create('work', { workspaceId: workspace.workspaceId });
      const initial = await firstValueFrom(programNames.pipe(filter(names => names.includes('mcp__Inventory-North__Stock-Count')), timeout(3_000)));
      assert.equal(await mcp.toolNames.read(), initial, 'hooks与普通函数读取同一个资源快照');
      const agent = runtime.native.get('agents')!.get(SessionId(session.id))!;
      assert.ok(runtime.native.get('tools')!.schemas(agent).some(tool => tool.name === 'mcp__Inventory-North__Stock-Count'));
      assert.ok(runtime.native.get('tools')!.schemas(agent).some(tool => tool.name === 'job_list'));

      const added = namesContaining('mcp__Inventory-North__lookup');
      const lookup = server.registerTool('lookup', { description: '新注册查询', inputSchema: {} }, async () => {
        executions++; return { content: [] };
      });
      await added;
      await firstValueFrom(programNames.pipe(filter(names => names.includes('mcp__Inventory-North__lookup')), timeout(3_000)));
      const removed = firstValueFrom(mcp.toolNames.values$.pipe(filter(names => !names.includes('mcp__Inventory-North__Stock-Count')), timeout(3_000)));
      stock.remove(); await removed;
      await firstValueFrom(programNames.pipe(filter(names => !names.includes('mcp__Inventory-North__Stock-Count')), timeout(3_000)));
      const empty = firstValueFrom(mcp.toolNames.values$.pipe(filter(names => names.length === 0), timeout(3_000)));
      lookup.remove(); await empty;
      await firstValueFrom(programNames.pipe(filter(names => names.length === 0), timeout(3_000)));
      assert.equal(runtime.native.get('tools')!.schemas(agent).some(tool => tool.name.startsWith('mcp__Inventory-North__')), false);

      await runtime.application.close(session.id); session.release();
      subscription.unsubscribe();
      assert.equal(mcp.toolNames.peek().status, 'pending', '最后消费者退出只撤下查询快照');
      const connected = namesContaining('mcp__Inventory-North__connected');
      server.registerTool('connected', { description: '查询退出后连接仍存活', inputSchema: {} }, async () => {
        executions++; return { content: [] };
      });
      await connected;
      assert.equal(mcp.toolNames.peek().status, 'pending');

      let complete = false;
      const observer = mcp.toolNames.values$.subscribe({ complete() { complete = true; } });
      await mcp.dispose();
      assert.equal(complete, true); assert.equal(observer.closed, true);
      assert.throws(() => mcp.toolNames.peek());
      assert.throws(() => mcp.toolNames.read());
      const late: (readonly string[])[] = [];
      mcp.toolNames$.subscribe(value => late.push(value));
      assert.deepEqual(late, [], '已退出能力不向迟到订阅重放工具名');
      assert.ok((await jobs.toolNames.read()).includes('job_list'), 'MCP退出不撤销Jobs安装');
      assert.equal(models, 0); assert.equal(executions, 0); assert.deepEqual(errors, []);
    } finally {
      subscription?.unsubscribe(); programNames.complete(); snapshots.complete();
      await runtime?.dispose(); await mcp.dispose(); await jobs.dispose();
      await server.close(); http.closeAllConnections();
      await new Promise<void>((resolve, reject) => http.close(error => error ? reject(error) : resolve()));
    }
  });
}
