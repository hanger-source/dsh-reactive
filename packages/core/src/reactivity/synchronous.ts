/** 同步派生不能接纳 Promise；异步工作通过资源或标准 Observable 表达。 */
export function synchronous<T>(value: T, operation: string): T {
  if (value !== null && (typeof value === 'object' || typeof value === 'function')
    && typeof (value as { then?: unknown }).then === 'function') {
    // 回调已经返回的异步工作不再有消费者，消费其拒绝，避免制造未处理异常。
    void Promise.resolve(value).catch(() => {});
    throw new TypeError('REACTIVE_' + operation + '_MUST_BE_SYNCHRONOUS');
  }
  return value;
}
