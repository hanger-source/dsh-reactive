import { RemoteError, type RemoteErrorCode } from '@deepseek-ai/dsh-typert-protocol';
import { z } from 'zod';
import type { ActionIdentity, NativeChildCommand, NativeChildReference, ProgramContractIdentity } from '@hanger-source/dsh-reactive';
import type { ProgramCreationReference, ProgramFailure } from './types.ts';

declare module '@deepseek-ai/dsh-typert-protocol' {
  interface RemoteErrorDetailsMap {
    'reactive/creation-incomplete': { readonly reference: ProgramCreationReference };
    'reactive/child-creation-incomplete': { readonly command: NativeChildCommand; readonly reference: NativeChildReference };
    'reactive/action-binding-unavailable': { readonly name: string; readonly reason: 'not-exposed' | 'binding-changed' | 'connection-lost' };
    'reactive/action-target-mismatch': { readonly name: string; readonly reason: 'target-mismatch' };
    'reactive/action-contract-mismatch': { readonly expected: ActionIdentity; readonly actual: ActionIdentity };
    'reactive/program-contract-mismatch': { readonly expected: ProgramContractIdentity; readonly actual: ProgramContractIdentity };
    'reactive/shared-program-required': Record<string, never>;
    'reactive/input-edit-rejected': { readonly requestId: string; readonly reason: 'unavailable' | 'actor-mismatch' | 'content-conflict' | 'changed' };
  }
}
/** 正式 failure 已由 Gateway/贡献合同核验，客户端保留完整公开 details。 */
export function decodeProgramFailure(value: Omit<ProgramFailure, 'details'> & { readonly details: unknown }): RemoteError {
  const details = z.union([z.array(z.json()), z.record(z.string(), z.json())]).parse(value.details);
  return new RemoteError(value.code as RemoteErrorCode, value.message, details);
}
