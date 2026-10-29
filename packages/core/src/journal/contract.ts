import type { JsonValue } from '@deepseek-ai/dsh-util-values';
import type { CancellationOptions } from '../conversations/protocol.ts';

/** 原生日志的公开传输形态；不把瞬态 ConversationEvent 重建为持久历史。 */
export interface ProgramJournalEvent {
  readonly type: string;
  readonly seq: number;
  readonly time: number;
  readonly data: JsonValue;
  readonly ignorable?: true;
  readonly sourceEventSeqs?: JsonValue;
  readonly surfaceOp?: JsonValue;
}
export interface ProgramJournalEntry { readonly type: 'event'; readonly event: ProgramJournalEvent; }
export interface ProgramJournalPage { readonly records: readonly ProgramJournalEntry[]; readonly hasMore: boolean; }
export interface ProgramJournalWindow {
  readonly maxMessages?: number;
  readonly turnWindow?: { readonly minMessages: number; readonly minTurns: number };
}
/** throughSeq 是同一 follow 开场给出的包含式原生日志水位。 */
export interface ProgramJournalPageQuery extends ProgramJournalWindow {
  readonly throughSeq: number;
  readonly beforeSeq?: number;
}
export type ProgramJournalFrame = { readonly type: 'opened'; readonly cursor: number; readonly page: ProgramJournalPage }
  | { readonly type: 'entry'; readonly entry: ProgramJournalEntry };

/** 只读原生日志；不唤醒 Agent，不拥有原模型、Action 或业务 Operation。 */
export interface SessionJournal {
  page(query: ProgramJournalPageQuery, options?: CancellationOptions): Promise<ProgramJournalPage>;
  /** 先取得冷安全开场，再交付此后原生追加；来源退出保留其资格或取消原因。 */
  follow(window?: ProgramJournalWindow, options?: CancellationOptions): AsyncIterable<ProgramJournalFrame>;
}
