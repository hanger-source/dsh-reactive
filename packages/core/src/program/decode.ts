import { ownConversationEvent, type ConversationEvent } from '../conversations/events.ts';
import { inputReferenceSchema, type InputReference } from '../conversations/protocol.ts';
import { assertProgramDefinition, type ProgramContract, type ProgramParsedInput } from './definition.ts';

/** 只解析本会话真正消费的输入；跨会话 cause 保留自己的原始合同与来源。 */
export function decodeProgramInputs<P extends ProgramContract>(program: P, sessionId: string,
  rawInputs: readonly InputReference[],
): readonly InputReference<ProgramParsedInput<P>>[] {
  assertProgramDefinition(program);
  if (!sessionId.trim()) throw new Error('REACTIVE_PROGRAM_INPUT_SESSION_REQUIRED');
  return Object.freeze(rawInputs.map(raw => {
    const input = inputReferenceSchema.parse(raw);
    if (input.sourceSessionId !== sessionId) throw new Error('REACTIVE_PROGRAM_INPUT_SESSION_MISMATCH');
    return Object.freeze({ ...input, scope: program.input.parse(input.scope) as ProgramParsedInput<P> });
  }));
}

/** 同一纯合同用于本地和远程读侧；输入原值不从 transform 的结果反向推导。 */
export function decodeProgramEvent<P extends ProgramContract>(program: P,
  raw: ConversationEvent,
): ConversationEvent<ProgramParsedInput<P>> {
  assertProgramDefinition(program);
  const event = ownConversationEvent(raw);
  if (event.kind === 'execution.error' || event.kind === 'assistant.message') {
    const { inputs, ...fact } = event;
    return inputs === undefined ? fact : { ...fact, inputs: decodeProgramInputs(program, event.sessionId, inputs) };
  }
  if ('inputs' in event) return { ...event, inputs: decodeProgramInputs(program, event.sessionId, event.inputs) };
  return event;
}
