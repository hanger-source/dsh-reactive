import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ReplaySubject, filter, firstValueFrom, timeout, type Subscription } from 'rxjs';
import type { ToolDefinition } from '@deepseek-ai/dsh-tools';
import { combineResources, defineApplication, defineEnvironment, defineProgram, defineSurface, z } from '@hanger-source/dsh-reactive';
import { implementProgram } from '@hanger-source/dsh-reactive/program';
import type { ConversationHandle } from '@hanger-source/dsh-reactive-dsh';
import { createNativeTools, type ToolSchema } from '@hanger-source/dsh-reactive-dsh/tools';
import { createNativeSkills } from '@hanger-source/dsh-reactive-dsh/skills';
import { nativeSessionAccess } from '../packages/dsh/src/conversations/session-access.ts';
import { boot, text } from './native-support.ts';

for (const mode of ['native', 'ptc'] as const) {
  test(`${mode} 工具目录观察真实注册、作用域遮蔽和restriction；组合Skill与恢复不执行模型或工具`, { timeout: 20_000 }, async () => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-native-tools-catalog-'));
    const program = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) });
    const definition = defineApplication({ name: 'native-tools-catalog', admit: async () => {}, environments: [defineEnvironment({
      id: 'work', instructions: '目录观察不执行工具或模型', program: implementProgram(program, { Component: () => null }),
    })] });
    let models = 0, executions = 0;
    const app = await boot(root, definition, async function* () { models++; yield* text('unused'); }, mode);
    const tools = createNativeTools({ service: 'provided' }), skills = createNativeSkills({ service: 'provided' });
    const definitionOf = (name: string, description: string): ToolDefinition => ({ name, description,
      parameters: { type: 'object', properties: {}, additionalProperties: false },
      output: { schema: { type: 'null' }, render: () => [{ type: 'text', text: 'null' }] },
      async execute() { executions++; return null; },
    });
    const registry = app.native.get('tools')!;
    const removeShared = registry.register(definitionOf('shared_tool', 'host-shared'));
    const removeMasked = registry.register(definitionOf('masked_tool', 'host-masked'));
    let one: ConversationHandle<typeof program> | undefined, two: ConversationHandle<typeof program> | undefined;
    let resumed: ConversationHandle<typeof program> | undefined, subscription: Subscription | undefined;
    try {
      await tools.install(app.native, root); await skills.install(app.native, root);
      one = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
      two = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
      let first = tools.forSession(one);
      const second = tools.forSession(two);
      assert.equal(tools.forSession(one), first);
      assert.throws(() => tools.forSession({ id: one!.id }), /REACTIVE_NATIVE_SESSION_REQUIRED/);
      const snapshots = new ReplaySubject<readonly ToolSchema[]>(1);
      subscription = first.catalog.values$.subscribe(snapshots);
      const initial = await first.catalog.read();
      assert.equal(initial.find(tool => tool.name === 'shared_tool')?.description, 'host-shared');
      assert.equal(initial.some(tool => tool.name === 'masked_tool'), true);
      assert.equal(Object.hasOwn(initial.find(tool => tool.name === 'shared_tool')!, 'execute'), false);
      assert.equal(Object.hasOwn(initial.find(tool => tool.name === 'shared_tool')!, 'output'), false);
      assert.equal(Reflect.set(initial.find(tool => tool.name === 'shared_tool')!, 'description', 'mutated'), false);
      assert.equal(registry.get('shared_tool')!.description, 'host-shared');

      const agent = nativeSessionAccess(one).liveAgent()!;
      agent.ctx.get('tools')!.register(definitionOf('shared_tool', 'agent-private'));
      await firstValueFrom(snapshots.pipe(filter(value => value.some(tool => tool.description === 'agent-private')), timeout(3_000)));
      assert.equal((await second.catalog.read()).find(tool => tool.name === 'shared_tool')?.description, 'host-shared');
      const lift = agent.ctx.get('tools')!.restrict({ deny: ['masked_tool'] });
      await firstValueFrom(snapshots.pipe(filter(value => !value.some(tool => tool.name === 'masked_tool')), timeout(3_000)));
      assert.equal((await second.catalog.read()).some(tool => tool.name === 'masked_tool'), true);
      lift();
      await firstValueFrom(snapshots.pipe(filter(value => value.some(tool => tool.name === 'masked_tool')), timeout(3_000)));

      const removeLate = registry.register(definitionOf('late_tool', 'late-registration'));
      await firstValueFrom(snapshots.pipe(filter(value => value.some(tool => tool.name === 'late_tool')), timeout(3_000)));
      removeLate();
      await firstValueFrom(snapshots.pipe(filter(value => !value.some(tool => tool.name === 'late_tool')), timeout(3_000)));
      const combined = await firstValueFrom(combineResources({ tools: first.catalog.observations$,
        skills: skills.forSession(one).catalog.observations$ }).pipe(filter(value => value.status === 'ready'), timeout(3_000)));
      assert.equal(combined.status, 'ready');
      if (combined.status === 'ready') {
        assert.equal(combined.value.tools.some(tool => tool.name === 'shared_tool'), true);
        assert.equal(combined.value.skills.complete, true);
      }
      // PTC只折叠模型直调入口；目录仍表达原生作用域完整可见schema。
      if (mode === 'ptc') assert.ok((await first.catalog.read()).length > 1);
      const retainMask = agent.ctx.get('tools')!.restrict({ deny: ['masked_tool'] });
      const sessionId = one.id;
      one.release();
      assert.throws(() => first.catalog.peek());
      assert.equal(agent.ctx.get('tools')!.schemas(agent).some(tool => tool.name === 'masked_tool'), false,
        'SDK释放不能解除其他owner的restriction');
      assert.equal(registry.get('shared_tool')?.description, 'host-shared');
      retainMask(); subscription.unsubscribe();
      const previous = first;
      one = await app.host.open(sessionId, program);
      first = tools.forSession(one);
      assert.notEqual(first, previous);
      subscription = first.catalog.values$.subscribe(snapshots);
      await first.catalog.read();
      await app.host.close(one.id);
      assert.equal(first.catalog.peek().status, 'pending');
      const cancellation = new AbortController();
      const waiting = first.catalog.read(cancellation.signal);
      cancellation.abort(new Error('cancel-only-this-read'));
      await assert.rejects(waiting, /cancel-only-this-read/);
      assert.equal(nativeSessionAccess(one).liveAgent(), undefined);
      resumed = await app.host.resume(one.id, program);
      assert.equal((await first.catalog.read()).find(tool => tool.name === 'shared_tool')?.description, 'host-shared');
      assert.equal((await second.catalog.read()).find(tool => tool.name === 'shared_tool')?.description, 'host-shared');
      assert.equal(models, 0); assert.equal(executions, 0); assert.deepEqual(app.errors, []);
    } finally {
      subscription?.unsubscribe(); removeMasked(); removeShared();
      resumed?.release(); one?.release(); two?.release();
      await tools.dispose(); await skills.dispose(); await app.dispose();
    }
  });
}
