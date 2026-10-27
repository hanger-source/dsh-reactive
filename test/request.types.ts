import { joinAssistantStreamText, type Conversation, type RequestHandle, type RequestSnapshot } from '@hanger-source/dsh-reactive';

function consume(conversation: Conversation<{ ticket: string }>) {
  const request: RequestHandle<{ ticket: string }> = conversation.request('page-request');
  request.events$.subscribe(event => {
    if (event.kind === 'assistant.message' || event.kind === 'assistant.stream') {
      event.inputs[0]?.scope.ticket.toUpperCase();
      // @ts-expect-error 本地事件保留领域输入类型，不会擦成 JSON 或原始数字。
      event.inputs[0]?.scope.ticket.toFixed();
    }
    if (event.kind === 'tool.started') event.parentCallId?.toUpperCase();
    if (event.kind === 'execution.error') event.inputs[0]?.scope.ticket.toUpperCase();
    // @ts-expect-error 请求事件仅来自已确认关联，不把全会话 Goal 变化冒充请求独占。
    const unrelated: 'goal.changed' = event.kind;
    void unrelated;
  });
  request.state$.subscribe(observed => {
    if (observed.status !== 'ready') return;
    const state: RequestSnapshot = observed.value;
    state.activity?.tools[0]?.parentCallId?.toUpperCase();
    if (state.activity) {
      // @ts-expect-error 请求事实由原生投影拥有，observer不能改写状态。
      state.activity.state = 'settled';
      // @ts-expect-error 工具快照不可由observer添加伪造执行。
      state.activity.tools.push({});
    }
    for (const attempt of state.attempts) {
      joinAssistantStreamText(attempt.stream).toUpperCase();
      // @ts-expect-error snapshot 元数据保持原生 JSON，不声称已经按领域输入 codec 解码。
      attempt.inputs[0]?.scope.ticket.toUpperCase();
    }
  });
  // @ts-expect-error 请求观察不取得原生工作控制权。
  request.completeGoal();
  // @ts-expect-error 观察请求不会再次提交输入。
  request.submit({});
}
void consume;
