import { map } from 'rxjs';
import { defineApplication, defineEnvironment, defineProgram, defineSurface, assertSharedProgramDefinition, z, type ProgramContract, type ProgramDefinition, type SharedProgramContract, type ProgramInput, type ProgramParsedInput, type ProgramSurface, type CreationReference } from '@hanger-source/dsh-reactive';
import { implementProgram } from '@hanger-source/dsh-reactive/program';
import type { ApplicationHost } from '@hanger-source/dsh-reactive-dsh';
import { createProgramClient, createServiceProgramClient, bindProgramClient, bindServiceProgramClient, type ProgramClientOptions } from '@hanger-source/dsh-reactive-client';
import type { Context as NativeContext } from '@deepseek-ai/cordis';

const surface = defineSurface({ views: {}, actions: {} });
const program = defineProgram({ input: z.string().transform(value => new Date(value)), surface });
const contract: ProgramContract = program;
const raw: ProgramInput<typeof program> = '2026-10-27T00:00:00Z';
const parsed: ProgramParsedInput<typeof program> = new Date(raw);
const publicSurface: ProgramSurface<typeof program> = surface;
const shared = defineProgram({ name: 'shared_date_contract', input: program.input, surface });
const published: SharedProgramContract = shared;
// @ts-expect-error 局部定义不取得发布者声明的跨进程身份。
const unpublished: SharedProgramContract = program;

function remoteBoundary(options: ProgramClientOptions, context: NativeContext) {
  void createProgramClient(shared, options);
  void createServiceProgramClient(shared, options);
  void bindProgramClient(shared, context);
  void bindServiceProgramClient(shared, context);
  const erased: ProgramDefinition<typeof shared.input, typeof shared.surface> = shared;
  assertSharedProgramDefinition(erased);
  void createProgramClient(erased, options);
  // @ts-expect-error 共享身份复核不能把原有精确Surface扩成任意槽索引。
  erased.surface.views.unknownSlot;
  // @ts-expect-error 远程构造必须使用明确共享的合同，不能默认为局部定义生成身份。
  void createProgramClient(program, options);
  // @ts-expect-error 服务端口也必须声明共享合同。
  void createServiceProgramClient(program, options);
  // @ts-expect-error 借入Context不改变共享合同要求。
  void bindProgramClient(program, context);
  // @ts-expect-error 服务Context绑定不允许无名Program。
  void bindServiceProgramClient(program, context);
}

const implementation = implementProgram(program, {
  Component({ conversation, workspace }) {
    workspace.directory.toUpperCase();
    // @ts-expect-error 同一工作区可运行多个环境，环境身份只属于当前会话。
    workspace.environmentId;
    conversation.identity.environmentId.toUpperCase();
    const inspect$ = conversation.inputs$.pipe(map(inputs => inputs.map(input => {
      input.scope.getUTCFullYear();
      // @ts-expect-error 领域输入保持 schema 的 Date output。
      input.scope.trim();
      return input.sourceSessionId;
    })));
    const submit = () => conversation.submit({ requestId: 'date', content: '读取日期', intent: 'question', delivery: 'followup' as const, scope: raw });
    // @ts-expect-error 显式提交保持 schema 的 string input，不重投领域 Date。
    const submitParsed = () => conversation.submit({ requestId: 'date-parsed', content: '读取日期', intent: 'question', delivery: 'followup' as const, scope: parsed });
    void inspect$; void submit; void submitParsed;
    return null;
  },
});

const environment = defineEnvironment({ id: 'date', instructions: '读取日期', program: implementation });
const application = defineApplication({ name: 'program-contract', environments: [environment], admit: async () => {} });
// @ts-expect-error 共享合同不携带宿主组件。
defineProgram({ input: z.null(), surface, Component: () => null });
// @ts-expect-error 环境必须装配真实宿主实现。
defineEnvironment({ id: 'unimplemented', instructions: '读取日期', program });
defineEnvironment({ id: 'owns-no-workspace', instructions: '读取日期', program: implementation,
  // @ts-expect-error 环境合同不拥有工作目录。
  directory: '/application/date' });
// @ts-expect-error 创建恢复引用必须明确原工作区。
const incompleteReference: CreationReference = { environmentId: 'date', sessionId: 'persisted-session' };

async function observe(host: ApplicationHost<typeof application.environments>) {
  const dynamic = await host.open('persisted-session');
  const dynamicEvents$ = dynamic.events$.pipe(map(event => {
    if ('inputs' in event && event.inputs) for (const input of event.inputs) {
      // @ts-expect-error 动态会话未声明预期 Program，领域 scope 保持 unknown。
      input.scope.getUTCFullYear();
    }
    return event;
  }));
  const expected = await host.open('persisted-session', program);
  const referenced = await host.open('persisted-session', { contract: { name: 'shared_date_contract', version: 1 } });
  // @ts-expect-error 发布者引用只核共享身份，不能凭引用取得任意本地定义的泛型。
  await host.open<typeof program>('persisted-session', { contract: { name: 'shared_date_contract', version: 1 } });
  const expectedEvents$ = expected.request('date').events$.pipe(map(event => {
    if ('inputs' in event && event.inputs) for (const input of event.inputs) {
      input.scope.getUTCFullYear();
      // @ts-expect-error 预期 Program 经真实核验后给出 Date output。
      input.scope.trim();
    }
    return event;
  }));
  await expected.submit({ requestId: 'date', content: '读取日期', intent: 'question', delivery: 'followup' as const, scope: raw });
  // @ts-expect-error 具名会话仍提交原始 string，不提交 parsed Date。
  await expected.submit({ requestId: 'date-parsed', content: '读取日期', intent: 'question', delivery: 'followup' as const, scope: parsed });
  // @ts-expect-error 类型参数不能跳过实际 expected Program 对象核验。
  await host.open<typeof program>('persisted-session');
  // @ts-expect-error 环境名称不代替该会话实际 Program 合同。
  await host.open('persisted-session', 'date');
  const workspace = await host.createWorkspace({ directory: '/application/date' });
  const created = await host.create('date', { workspaceId: workspace.workspaceId });
  const resumed = await host.resume('persisted-session', program);
  const current = host.conversation('persisted-session', program);
  await created.submit({ requestId: 'created', content: '读取日期', intent: 'question', delivery: 'followup' as const, scope: raw });
  await resumed.submit({ requestId: 'resumed', content: '读取日期', intent: 'question', delivery: 'followup' as const, scope: raw });
  // @ts-expect-error 同步读取保持同一 Program input 合同。
  await current.submit({ requestId: 'current', content: '读取日期', intent: 'question', delivery: 'followup' as const, scope: parsed });
  void dynamicEvents$; void expectedEvents$; void referenced;
}

void contract; void publicSurface; void observe; void incompleteReference; void published; void unpublished; void remoteBoundary;
