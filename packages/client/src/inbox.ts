/** 有界 carrier 邮箱；只缓存尚未消费的帧，不承担重连、游标或历史。 */
export class Inbox<T> implements AsyncIterable<T> {
  #items: { value: T; bytes: number }[] = [];
  #bytes = 0;
  #ended = false;
  #failure?: { error: unknown };
  #wake?: () => void;
  constructor(private readonly maxBytes: number) {}
  push(value: T, bytes: number = new TextEncoder().encode(JSON.stringify(value)).byteLength): void {
    if (this.#ended) return;
    if (this.#bytes + bytes > this.maxBytes) {
      const error = new Error('REACTIVE_REMOTE_INBOX_OVERFLOW'); this.fail(error); throw error;
    }
    this.#items.push({ value, bytes }); this.#bytes += bytes; this.#wake?.();
  }
  end(): void { this.#ended = true; this.#wake?.(); }
  fail(error: unknown): void { this.#failure = { error }; this.#items.length = 0; this.#bytes = 0; this.end(); }
  async *[Symbol.asyncIterator](): AsyncGenerator<T> {
    try {
      for (;;) {
        if (this.#failure) throw this.#failure.error;
        const item = this.#items.shift();
        if (item) { this.#bytes -= item.bytes; yield item.value; continue; }
        if (this.#ended) return;
        await new Promise<void>(resolve => { this.#wake = resolve; });
        this.#wake = undefined;
      }
    } finally { this.#items.length = 0; this.#bytes = 0; this.end(); }
  }
}
