import { randomUUID } from 'node:crypto';
import type { Context } from '@deepseek-ai/cordis';
import { HarnessError, ToolCallId } from '@deepseek-ai/dsh-llm';
import { scopeTarget } from '@deepseek-ai/dsh-scope';
import type { ToolRunContext, ToolExecutionResult } from '@deepseek-ai/dsh-tools';
import { isTimedAskUserQuestionSchema } from '@deepseek-ai/dsh-user-questions';
import { z } from 'zod';
import { humanAnswerSchema, type HumanPrompt, type HumanAskOptions, type HumanAskResult } from '@hanger-source/dsh-reactive/kernel';

const nativeAnswerSchema = z.union([humanAnswerSchema, z.object({
  pending: z.literal(true), callId: z.string().min(1), message: z.string(),
}).strict().transform(({ pending, callId }) => ({ pending, callId }))]);

/** 执行本次 Action 真正发起的子工具调用；原生管线持有策略、问答与取消。 */
export async function dispatchHumanQuestion(ctx: Context, parent: ToolRunContext,
  questions: readonly HumanPrompt[], options: HumanAskOptions | undefined, callerSignal: AbortSignal): Promise<HumanAskResult> {
  const agent = parent.agent;
  if (!agent || ctx.agents.get(agent.id) !== agent) throw new Error('REACTIVE_ACTION_TOOL_AUTHORITY_REQUIRED');
  const tool = ctx.tools.get('ask_user_question', agent);
  if (!tool || !isTimedAskUserQuestionSchema(tool)) throw new Error('REACTIVE_HUMAN_TIMED_TOOL_REQUIRED');
  const signal = AbortSignal.any([parent.signal, callerSignal]);
  signal.throwIfAborted();
  const args = { questions: questions.map(question => ({
    id: question.id, question: question.question,
    ...question.header === undefined ? {} : { header: question.header },
    ...question.options === undefined ? {} : { options: question.options.map(option => ({
      label: option.label, ...option.description === undefined ? {} : { description: option.description },
    })) },
    ...question.multiSelect === undefined ? {} : { multi_select: question.multiSelect },
  })), timeout: options === undefined ? -1 : options.timeoutSeconds };
  const subCallId = ToolCallId(randomUUID());
  const dispatch = { rootCallId: parent.rootCallId, parentCallId: parent.callId,
    subCallId, name: 'ask_user_question', arguments: args };
  // programmatic 子调用不是模型直接调用；日志记录真实执行的起止，不生成 tool/call。
  agent.session.append('tool/ptc-dispatch-start', dispatch);
  const result = await ctx.tools.execute({ callId: subCallId, rootCallId: parent.rootCallId,
    parent: parent.token, agent, name: dispatch.name, arguments: args, signal });
  for (const context of result.additionalContexts ?? []) parent.deferContext(context);
  if (result.concludesTurn) parent.concludeTurn();
  let content: ToolExecutionResult['content'];
  try {
    content = await ctx.waterfall(scopeTarget(ctx.tools, agent), 'tools/ptc-dispatch-log', {
      exec: parent, agent, subCallId, name: dispatch.name, isError: result.isError, content: result.content,
    }, () => Promise.resolve(result.content));
    agent.session.append('tool/ptc-dispatch', { ...dispatch, isError: result.isError, content,
      ...result.meta === undefined ? {} : { meta: result.meta },
      ...result.error?.info === undefined ? {} : { error: result.error.info },
    });
  } catch (error) {
    console.error('reactive.human.dispatch.log.failed', { sessionId: agent.id, callId: subCallId, error });
    const message = 'Human question tool pipeline settled, but its dispatch log policy failed';
    const failure = Object.assign(new HarnessError(message, 'REACTIVE_HUMAN_DISPATCH_LOG_FAILED'), {
      name: 'HumanDispatchLogError', cause: error,
    });
    // 工具已经实际执行；本次 end 明确归日志阶段失败，不公开未经策略处理的原始结果。
    agent.session.append('tool/ptc-dispatch', { ...dispatch, isError: true,
      content: [{ type: 'text', text: 'Error: ' + message }],
      error: { name: failure.name, code: failure.code,
        reason: result.isError ? 'tool pipeline failed; dispatch log policy then failed'
          : 'tool pipeline returned; dispatch log policy then failed' },
    });
    throw failure;
  }
  if (result.isError) {
    const info = result.error.info;
    if (info) throw Object.assign(new HarnessError(result.error.message, info.code), { name: info.name,
      ...info.reason === undefined ? {} : { reason: info.reason } });
    throw new Error(result.error.message);
  }
  signal.throwIfAborted();
  const answer = nativeAnswerSchema.parse(result.value);
  if ('pending' in answer && answer.callId !== subCallId) throw new Error('REACTIVE_HUMAN_QUESTION_CALL_MISMATCH');
  return answer;
}
