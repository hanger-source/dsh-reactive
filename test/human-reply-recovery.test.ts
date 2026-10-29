import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { filter, firstValueFrom } from 'rxjs';
import { SessionId } from '@deepseek-ai/dsh-session';
import { defineApplication, defineEnvironment, defineProgram, defineSurface, z } from '@hanger-source/dsh-reactive';
import { HumanInput, implementProgram, useBeforeStep } from '@hanger-source/dsh-reactive/program';
import { boot, text, tool } from './native-support.ts';

const question = [{ id: 'permission', question: '是否允许继续处理？' }];
const answer = { answers: [{ id: 'permission', selected: ['允许'] }] };
const program = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) });
const checkpointSchema = z.object({ id: z.string(), replyIds: z.string().array(), questionCallIds: z.string().array(), failureSequence: z.number() }).strict();

async function scenario(root: string, reject: () => boolean, questionCount: number) {
  let calls = 0;
  const definition = defineApplication({ name: 'human-reply-crash', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '许可超时后可先结束独立工作；晚答必须保留原消息身份',
    program: implementProgram(program, { Component() {
      useBeforeStep(() => { if (reject()) throw new Error('REPLY_ASSEMBLY_FAILED'); }, []);
      return HumanInput({ admitResponder() {} });
    } }),
  })] });
  const app = await boot(join(root, 'home'), definition, async function* (request) {
    calls++;
    const asked = request.messages.flatMap(message => message.content).filter(block => block.type === 'tool-call' && block.name === 'ask_user_question').length;
    if (asked < questionCount) yield* tool('ask_user_question', 'crash-question-' + asked, { questions: question, timeout: 1 });
    else yield* text('已完成当前明确要求');
  });
  return { ...app, calls: () => calls };
}

async function checkpointWorker(root: string, questionCount: number) {
  let reject = false;
  const app = await scenario(root, () => reject, questionCount);
  const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
  const foreground = firstValueFrom(session.foregroundQuestions$.pipe(filter(value => value.length === 1)));
  await session.submit({ requestId: 'original', content: '询问许可', intent: 'question', delivery: 'followup', scope: null });
  const [pending] = await foreground;
  assert.ok(pending);
  await session.whenIdle();
  const native = app.native.agents.get(SessionId(session.id));
  assert.ok(native);
  const prepend = native.inbox.prepend.bind(native.inbox);
  // 在真实持久 Inbox 接口注入写入故障，进程停在 failure 已持久而原消息尚未交回的边界。
  native.inbox.prepend = (target, message) => {
    if (message.source.kind === 'user-question-reply') throw new Error('REPLY_INBOX_WRITE_INTERRUPTED');
    prepend(target, message);
  };
  reject = true;
  const pendingQuestions = (await session.human.read()).questions.active;
  assert.equal(pendingQuestions.length, questionCount);
  await native.runMaintenance(async () => {
    for (const item of pendingQuestions) assert.equal(await session.answerQuestion(item.callId, answer), true);
  });
  await native.whenIdle();
  assert.equal(await app.native.sessions.flush(native.session), true);
  const [failure] = session.snapshot().failures;
  assert.ok(failure);
  assert.equal(failure.unentered.length, questionCount);
  assert.deepEqual((await session.human.read()).replies, []);
  assert.ok(!native.session.snapshotEvents().some(event => event.type === 'user/message' && failure.unentered.some(item => item.messageId === event.data.id)));
  process.send!(checkpointSchema.parse({ id: session.id, replyIds: failure.unentered.map(item => item.messageId),
    questionCallIds: failure.unentered.map(item => item.questionCallId), failureSequence: failure.sequence }));
  process.on('message', () => {});
  await new Promise<void>(() => {});
}

if (process.argv[2] === '--human-reply-checkpoint') await checkpointWorker(process.argv[3]!, Number(process.argv[4]));
else for (const cancel of [false, true]) test(cancel ? '冷激活恢复双晚答时首条正式插入回调取消，停止其余恢复并持久取消原消息'
  : '失败交接写入前进程硬退出，只读冷开不重放，实际激活恢复并消费原晚答一次', { timeout: 20_000 }, async context => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-human-reply-crash-'));
  const count = cancel ? 2 : 1;
  const worker = fork(import.meta.filename, ['--human-reply-checkpoint', root, String(count)], {
    execPath: process.execPath, execArgv: [], stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
  });
  let output = '';
  worker.stdout?.on('data', chunk => { output += String(chunk); });
  worker.stderr?.on('data', chunk => { output += String(chunk); });
  const checkpoint = new Promise<z.output<typeof checkpointSchema>>((resolve, reject) => {
    worker.once('message', message => { try { resolve(checkpointSchema.parse(message)); } catch (error) { reject(error); } });
    worker.once('error', reject);
    worker.once('exit', (code, signal) => reject(new Error(`REPLY_WORKER_EARLY_EXIT ${code}/${signal}\n${output}`)));
  });
  const abort = () => { worker.kill('SIGKILL'); };
  context.signal.addEventListener('abort', abort, { once: true });
  let saved: z.output<typeof checkpointSchema>;
  try {
    saved = await checkpoint;
    const exited = once(worker, 'exit');
    worker.kill('SIGKILL');
    assert.deepEqual(await exited, [null, 'SIGKILL']);
  } finally { context.signal.removeEventListener('abort', abort); worker.kill('SIGKILL'); }
  const app = await scenario(root, () => false, count);
  try {
    const cold = await app.host.open(saved.id, program);
    try {
      assert.equal(app.native.agents.get(SessionId(saved.id)), undefined);
      assert.deepEqual((await cold.human.read()).replies, []);
      assert.equal((await cold.human.read()).questions.active[0]?.state, 'continued');
      assert.equal(cold.snapshot().failures[0]?.sequence, saved.failureSequence);
      assert.equal(app.calls(), 0, '只读 open 没有激活或模型调用');
      const restored: string[] = [];
      const stop = app.native.on('agent/inbox/inserted', ({ agent, message }) => {
        if (!cancel || !saved.replyIds.includes(message.id)) return;
        restored.push(message.id);
        agent.cancel({ kind: 'user' });
      });
      try {
        await cold.submit({ requestId: 'resume', content: '明确继续已受理的原答案', intent: 'question', delivery: 'followup', scope: null });
        await cold.whenIdle();
      } finally { stop(); }
      const native = app.native.agents.get(SessionId(saved.id));
      assert.ok(native);
      assert.equal(native.session.snapshotEvents().filter(event => event.type === 'user/message' && saved.replyIds.includes(event.data.id)).length, cancel ? 0 : 1);
      const human = await cold.human.read();
      assert.deepEqual(human.replies, []);
      if (cancel) {
        assert.equal(restored.length, 1, 'idle cancel 不必撤销生命周期 signal；正式 discarded 必须终止后续恢复');
        assert.equal(human.questions.active.length, 2);
        assert.deepEqual(human.questions.settled, []);
        const cancelledId = restored[0]!;
        await app.host.close(saved.id);
        const reopened = await app.host.open(saved.id, program);
        try {
          await reopened.submit({ requestId: 'after-cancel', content: '再次明确继续尚未取消的原工作',
            intent: 'question', delivery: 'followup', scope: null });
          await reopened.whenIdle();
          const resumed = app.native.agents.get(SessionId(saved.id));
          assert.ok(resumed);
          assert.ok(!resumed.session.snapshotEvents().some(event => event.type === 'user/message' && event.data.id === cancelledId),
            '后续激活仍不得复活已有正式 canceled 事实的原 ID');
          assert.equal(resumed.session.snapshotEvents().filter(event => event.type === 'user/message'
            && saved.replyIds.includes(event.data.id)).length, 1, '同批另一条没有取消事实的原答案可沿自己的 claim 恢复');
        } finally { reopened.release(); }
      } else {
        assert.deepEqual(human.questions.active, []);
        assert.deepEqual(human.questions.settled.map(value => [value.callId, value.answers]), [[saved.questionCallIds[0], answer.answers]]);
        assert.equal(app.calls(), 1, '明确激活后只消费一次原答案，没有重新提问');
      }
      assert.equal(cold.snapshot().failures[0]?.sequence, saved.failureSequence);
    } finally { cold.release(); }
  } finally { await app.dispose(); }
});
