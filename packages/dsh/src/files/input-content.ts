import type { Context } from '@deepseek-ai/cordis';
import type { Agent } from '@deepseek-ai/dsh-agent';
import type { AttachmentAdmissionPart } from '@deepseek-ai/dsh-attachment';
import type {} from '@deepseek-ai/dsh-client-file-upload';
import type { FileUploadReceiptId } from '@deepseek-ai/dsh-client-file-upload/types';
import type { AdmittedPromptContentPart, PromptContentPart } from '@hanger-source/dsh-reactive/kernel';

/** 未签发给本次 exact Session 的 receipt 不能升级为持久文件输入。 */
export class InputFileNotStagedError extends Error {
  constructor() { super('REACTIVE_INPUT_FILE_NOT_STAGED'); this.name = 'InputFileNotStagedError'; }
}

/** @internal receipt 只在原生 exact Session 中解析；文件绑定与投递事务由受理方持有。 */
export async function admitInputContent(ctx: Context, agent: Agent | undefined,
  parts: readonly PromptContentPart[]): Promise<{
    readonly content: readonly AdmittedPromptContentPart[];
    readonly receiptIds: readonly FileUploadReceiptId[];
  }> {
  const receiptIds = new Set<FileUploadReceiptId>();
  const admission: AttachmentAdmissionPart[] = parts.map(part => {
    if (part.type !== 'file') return part;
    if (!agent) throw new InputFileNotStagedError();
    const attachment = ctx.fileUploads.resolve(agent, part.receiptId);
    if (!attachment) throw new InputFileNotStagedError();
    receiptIds.add(part.receiptId);
    return { type: 'file', attachment: structuredClone(attachment) };
  });
  return { content: await ctx.attachments.admitPromptContent(admission), receiptIds: [...receiptIds] };
}
