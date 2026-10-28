import { defineEnvironment, type AnyEnvironment } from '../environments/definition.ts';
import type { AdmittedSubmission, Admission } from '../conversations/protocol.ts';
import type { ConversationIdentity } from '../conversations/conversation.ts';

export interface ApplicationDefinition<E extends readonly AnyEnvironment[] = readonly AnyEnvironment[]> {
  readonly name: string;
  readonly environments: E;
  /** 首次投递与消费前复核均读取同一已提升正文，可信后台投递也经过此准入。 */
  admit(input: AdmittedSubmission, conversation: ConversationIdentity, admission: Admission): Promise<void>;
}
export function defineApplication<const E extends readonly AnyEnvironment[]>(definition: ApplicationDefinition<E>): ApplicationDefinition<E> {
  // 快照保持每个环境的合同与原Program实现，只捕获声明，保留调用者的tuple与id类型。
  const environments = Object.freeze(definition.environments.map(environment => defineEnvironment(environment))) as unknown as E;
  if (!definition.name || environments.length === 0) throw new Error('REACTIVE_APPLICATION_INVALID');
  if (new Set(environments.map(environment => environment.id)).size !== environments.length)
    throw new Error('REACTIVE_ENVIRONMENT_DUPLICATE');
  return Object.freeze({ ...definition, environments });
}
