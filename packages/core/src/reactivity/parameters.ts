import { z } from 'zod';
import type { Json } from '../conversations/protocol.ts';

/** @internal 参数实例按 JSON 领域含义寻址，不依赖对象字段插入顺序。 */
export function parameterIdentity(value: Json): string {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return '[' + value.map(parameterIdentity).join(',') + ']';
  return '{' + Object.entries(value).sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0)
    .map(([key, child]) => JSON.stringify(key) + ':' + parameterIdentity(child)).join(',') + '}';
}

/** @internal 保存自有参数快照；不再次求值业务 schema 的单向 transform。 */
export function ownParameters<T extends Json>(value: T): T {
  const snapshot = z.json().parse(value);
  const freeze = (current: Json): void => {
    if (current !== null && typeof current === 'object') {
      for (const child of Object.values(current)) freeze(child);
      Object.freeze(current);
    }
  };
  freeze(snapshot);
  // JSON 边界只复制已通过合同解析的 T，不增加或移除领域字段。
  return snapshot as T;
}
