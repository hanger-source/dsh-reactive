import { z } from 'zod';
import { assertSupportedJsonSchema, valueSchemaSpecToJsonSchema, type JsonSchemaNode } from '@deepseek-ai/dsh-tools';
import { canonical } from '../conversations/records.ts';

const annotations = new Set(['description', 'title', 'default', 'examples']);
const schemaObject = z.record(z.string(), z.unknown());
// 规范来自当前 Zod 自己的转换，不复制另一套 Zod AST 或递归 schema 编译器。
const jsonPattern = canonical(jsonShape(z.toJSONSchema(z.json(), { target: 'draft-7' }), '#'));

/** 完整 Zod 合同负责执行校验；原生声明采用 dsh 真正支持的 schema 子集。 */
export function toolSchema(schema: z.ZodType, io: 'input' | 'output' = 'input'): JsonSchemaNode & Record<string, unknown> {
  const raw = z.toJSONSchema(schema, { target: 'draft-7', io });
  const { definitions, ...body } = raw;
  const jsonReferences = new Map<string, Record<string, unknown>>();
  if (isJsonShape(body, '#')) jsonReferences.set('#', pickAnnotations(body));
  if (definitions !== undefined) {
    for (const [name, definition] of Object.entries(schemaObject.parse(definitions))) {
      const reference = '#/definitions/' + name;
      if (!isJsonShape(definition, reference)) throw new Error('REACTIVE_TOOL_SCHEMA_UNSUPPORTED: definitions/' + name);
      jsonReferences.set(reference, pickAnnotations(schemaObject.parse(definition)));
    }
  }
  const result = jsonReferences.has('#') ? jsonValue(pickAnnotations(body)) : project(body, jsonReferences);
  assertSupportedJsonSchema(result);
  return result;
}
const constraints = new Set(['minLength', 'maxLength', 'pattern', 'format', 'minimum', 'maximum',
  'exclusiveMinimum', 'exclusiveMaximum', 'minItems', 'maxItems', 'multipleOf']);
function project(raw: unknown, jsonReferences: ReadonlyMap<string, Record<string, unknown>>): JsonSchemaNode & Record<string, unknown> {
  const input = schemaObject.parse(raw);
  const reference = jsonReference(input);
  if (reference && jsonReferences.has(reference.ref))
    return jsonValue({ ...jsonReferences.get(reference.ref), ...reference.annotations });
  const output: Record<string, unknown> = {};
  const notes: string[] = input.description === undefined ? [] : [z.string().parse(input.description)];
  for (const [key, value] of Object.entries(input)) {
    if (key === 'description' || key === '$schema') continue;
    if (constraints.has(key)) { notes.push(key + '=' + JSON.stringify(value)); continue; }
    if (key === 'properties') output.properties = Object.fromEntries(Object.entries(z.record(z.string(), z.unknown()).parse(value))
      .map(([name, child]) => [name, project(child, jsonReferences)]));
    else if (key === 'items') output.items = project(value, jsonReferences);
    else if (key === 'propertyNames') {
      if (input.type !== 'object' || canonical(jsonShape(value, '')) !== canonical({ type: 'string' }))
        throw new Error('REACTIVE_TOOL_SCHEMA_UNSUPPORTED: propertyNames');
    }
    else if (key === 'additionalProperties') {
      if (typeof value === 'boolean') output.additionalProperties = value;
      else {
        const additional = project(value, jsonReferences);
        if (!isUnconstrained(additional)) throw new Error('REACTIVE_TOOL_SCHEMA_UNSUPPORTED: additionalProperties');
        output.additionalProperties = true;
      }
    }
    else if (key === 'anyOf' || key === 'oneOf') {
      const branches = z.array(z.unknown()).parse(value).map(branch => project(branch, jsonReferences));
      // anyOf 的任意 JSON 分支已包含其他分支；oneOf 仍保持 exact-one 语义。
      if (key === 'anyOf' && branches.some(isUnconstrained)) {
        for (const branch of branches.filter(isUnconstrained)) {
          if (branch.description !== undefined && !notes.includes(branch.description)) notes.push(branch.description);
        }
        continue;
      }
      if (key === 'anyOf' && !branches.every((left, index) => branches.slice(index + 1).every(right => disjoint(left, right)))) {
        throw new Error('REACTIVE_TOOL_SCHEMA_UNION_OVERLAPS');
      }
      output.oneOf = branches;
    }
    else if (['type', 'required', 'const', 'enum', 'title', 'default', 'examples'].includes(key)) output[key] = value;
    else throw new Error('REACTIVE_TOOL_SCHEMA_UNSUPPORTED: ' + key);
  }
  if (notes.length) output.description = notes.join('；');
  assertSupportedJsonSchema(output);
  return output;
}

function pickAnnotations(input: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(Object.entries(input).filter(([key]) => annotations.has(key)));
}
function jsonValue(annotation: Record<string, unknown>): JsonSchemaNode & Record<string, unknown> {
  const result = { ...valueSchemaSpecToJsonSchema({ type: 'json' }), ...annotation };
  assertSupportedJsonSchema(result);
  return result;
}
function isUnconstrained(input: JsonSchemaNode): boolean {
  return Object.keys(input).every(key => annotations.has(key));
}
/** 只读取纯引用或单引用 allOf；有额外约束的 allOf 不属于此映射。 */
function jsonReference(input: Record<string, unknown>): { ref: string; annotations: Record<string, unknown> } | undefined {
  const keys = Object.keys(input).filter(key => !annotations.has(key) && key !== '$schema');
  if (keys.length !== 1) return;
  if (keys[0] === '$ref' && typeof input.$ref === 'string') return { ref: input.$ref, annotations: pickAnnotations(input) };
  if (keys[0] === 'allOf' && Array.isArray(input.allOf) && input.allOf.length === 1) {
    const reference = jsonReference(schemaObject.parse(input.allOf[0]));
    if (reference) return { ref: reference.ref, annotations: { ...reference.annotations, ...pickAnnotations(input) } };
  }
}
/** 比较 Zod 输出图的确切结构；仅允许注解及等价的 self-ref 包装差异。 */
function jsonShape(raw: unknown, self: string): unknown {
  if (Array.isArray(raw)) return raw.map(value => jsonShape(value, self));
  if (raw === null || typeof raw !== 'object') return raw;
  const input = schemaObject.parse(raw);
  if (jsonReference(input)?.ref === self) return { $ref: '#' };
  return Object.fromEntries(Object.entries(input).filter(([key]) => !annotations.has(key) && key !== '$schema')
    .map(([key, value]) => [key, jsonShape(value, self)]));
}
function isJsonShape(raw: unknown, self: string): boolean {
  return canonical(jsonShape(raw, self)) === jsonPattern;
}

function disjoint(left: JsonSchemaNode, right: JsonSchemaNode): boolean {
  if (left.type && right.type && left.type !== right.type) {
    return !(['number', 'integer'].includes(left.type) && ['number', 'integer'].includes(right.type));
  }
  const values = (node: JsonSchemaNode) => node.const !== undefined ? [node.const] : node.enum;
  const a = values(left), b = values(right);
  if (a && b && a.every(value => !b.includes(value))) return true;
  if (left.type === 'object' && right.type === 'object') {
    return (left.required ?? []).some(key => right.required?.includes(key) && left.properties?.[key] && right.properties?.[key]
      && disjoint(left.properties[key]!, right.properties[key]!));
  }
  return false;
}
