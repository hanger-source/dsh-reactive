import { z, collectionSurface, composeSurface, defineAction, defineOperation, defineProgram, defineSurface, resourceView, view } from '@hanger-source/dsh-reactive';

export const date = z.codec(z.iso.datetime(), z.date(), { decode: value => new Date(value), encode: value => value.toISOString() });
export const operation = defineOperation({ kind: 'browser-check', version: 1, command: z.string(), reference: z.string(),
  state: z.object({ phase: z.literal('done'), at: date }), settled: state => state });
export const check = defineAction({ name: 'check', description: '明确受理浏览器发起的核验', scope: z.null(),
  input: z.object({ amount: z.string().transform(value => Number(value)).pipe(z.number()) }),
  output: z.object({ at: date, operation: z.string() }),
  operation: { definition: operation, command: input => String(input.amount), reference: receipt => receipt.operation } });
export const save = defineAction({ name: 'native_record', description: '保存当前模块的明确版本', scope: z.string(),
  input: z.string(), output: z.string(),
  operation: { definition: operation, command: (_input, scope) => scope, reference: receipt => receipt } });
export const local = defineSurface({ views: { record: resourceView(z.string()) }, actions: { save }, operations: { current: operation } });
export const rowKey = 'row/杭州.a';
export const surface = defineSurface({ views: { current: view(date) }, actions: { check } });
export const program = defineProgram({ name: 'browser_contracts', input: z.null(), surface });
export const layout = composeSurface({ root: surface, overview: local, rows: collectionSurface(local) });
export const nativeProgram = defineProgram({ name: 'browser_native_contracts', input: z.null(), surface: layout.surface });
