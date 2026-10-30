import { defineAction, defineProgram, defineSurface, nativeChildWorkAction, z } from '@hanger-source/dsh-reactive';
import { implementProgram } from '@hanger-source/dsh-reactive/program';
import type { ApplicationAssembly } from '@hanger-source/dsh-reactive-dsh';

const child = defineProgram({ input: z.object({ caseId: z.string(), at: z.iso.datetime().transform(value => new Date(value)) }).strict(),
  surface: defineSurface({ views: {}, actions: {} }) });
const childImplementation = implementProgram(child, { Component: () => null });
const delegate = defineAction({ ...nativeChildWorkAction, name: 'investigate', description: '调查当前对象',
  scope: z.object({ caseId: z.string() }).strict(), input: z.object({ at: z.iso.datetime().transform(value => new Date(value)) }).strict() });

function assemble(assembly: ApplicationAssembly) {
  const implementation = assembly.implementDelegation(delegate, {
    program: childImplementation,
    mode: 'spawn',
    childInput: (input, call) => ({ caseId: call.scope.caseId, at: input.at.toISOString() }),
    objective: (_input, call) => `调查 ${call.scope.caseId}`,
    authorize(admission) {
      admission.input.at.getUTCFullYear();
      admission.target.caseId.toUpperCase();
      admission.causes[0]?.sourceSessionId.toUpperCase();
      if (admission.phase === 'resume') {
        admission.recovery.child.childSessionId.toUpperCase();
        admission.recovery.messageId.toUpperCase();
        admission.recovery.goal?.objective.toUpperCase();
        // @ts-expect-error 恢复目标是自有不可变快照。
        admission.recovery.goal!.revision = 123;
      } else {
        // @ts-expect-error 非恢复准入没有伪造的恢复现场。
        admission.recovery;
      }
      // @ts-expect-error 授权读取领域Date，不退化成原始字符串。
      admission.input.at.trim();
      // @ts-expect-error 不把原生Agent暴露给业务授权代码。
      admission.agent;
    },
  });
  assembly.implementDelegation(delegate, { program: childImplementation, mode: 'fork',
    // @ts-expect-error 子程序原始输入由真实Program合同推导。
    childInput: () => ({ caseId: 123, at: '2026-10-27T00:00:00Z' }),
    objective: () => '调查', authorize() {},
  });
  assembly.implementDelegation(delegate, { program: childImplementation, mode: 'spawn',
    // @ts-expect-error 子程序提交使用string原值，不能提交解析后的Date。
    childInput: () => ({ caseId: 'case-a', at: new Date() }),
    objective: () => '调查', authorize() {},
  });
  return implementation;
}
void assemble;
