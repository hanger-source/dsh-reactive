import type { Context } from '@deepseek-ai/cordis';
import Schema from '@deepseek-ai/schemastery';
import { stripTypeScriptTypes } from 'node:module';
import { setImmediate as yieldToHost } from 'node:timers/promises';
import { PtcRuntime, PORTABLE_RESERVED_WORDS, RESERVED_BINDING_GLOBALS, RESERVED_ERROR_MEMBERS, DUNDER_MEMBER, type PtcBindingNamespace, type PtcJsonValue, type PtcRunFailure, type PtcRunRequest, type PtcRunResult, type PtcRunSpec } from '@deepseek-ai/dsh-ptc-runtime';
import { newQuickJSWASMModuleFromVariant, type QuickJSContext, type QuickJSRuntime, type QuickJSHandle, type QuickJSDeferredPromise } from 'quickjs-emscripten-core';
import variant from '@jitl/quickjs-singlefile-cjs-release-sync';

export interface Config {
  timeoutMs?: number;
  maxTimeoutMs?: number;
  cpuBudgetMs?: number;
  memoryLimitBytes?: number;
  maxStackSizeBytes?: number;
  maxOutputBytes?: number;
  maxBindingBytes?: number;
  maxPendingCalls?: number;
  maxConcurrentRuns?: number;
}
const defaults = { timeoutMs: 120_000, maxTimeoutMs: 600_000, cpuBudgetMs: 1_000,
  memoryLimitBytes: 32 * 1024 * 1024, maxStackSizeBytes: 256 * 1024,
  maxOutputBytes: 1024 * 1024, maxBindingBytes: 1024 * 1024, maxPendingCalls: 64, maxConcurrentRuns: 8 };
type Limits = typeof defaults;
const integer = () => Schema.number().min(1).step(1);

// These functions live only in QuickJS. No guest value is converted to a host object
// until it has crossed this lossless JSON boundary; no host object is exposed to it.
const guestCodec = `(() => {
  const stringify = JSON.stringify, parse = JSON.parse, keys = Reflect.ownKeys;
  const descriptors = Object.getOwnPropertyDescriptors, prototype = Object.getPrototypeOf;
  const create = Object.create, setPrototype = Object.setPrototypeOf, array = Array.isArray;
  const objectPrototype = Object.prototype, arrayPrototype = Array.prototype;
  const finite = Number.isFinite, StringValue = String;
  const SetValue = Set, has = Function.call.bind(Set.prototype.has);
  const add = Function.call.bind(Set.prototype.add), remove = Function.call.bind(Set.prototype.delete);
  function encode(value) {
    const seen = new SetValue();
    function copy(value, depth) {
      if (depth > 64) throw new TypeError('JSON nesting exceeds 64');
      if (value === null || typeof value === 'string' || typeof value === 'boolean') return value;
      if (typeof value === 'number' && finite(value)) return value;
      if (typeof value !== 'object' || has(seen, value)) throw new TypeError('Value must be lossless JSON');
      const isArray = array(value), proto = prototype(value);
      if (proto !== null && proto !== (isArray ? arrayPrototype : objectPrototype)) throw new TypeError('Value must be plain JSON');
      add(seen, value);
      const fields = descriptors(value), result = isArray ? [] : create(null);
      if (isArray) setPrototype(result, null);
      let count = 0;
      for (const key of keys(fields)) {
        if (isArray && key === 'length') continue;
        const field = fields[key];
        if (typeof key !== 'string' || !('value' in field) || !field.enumerable) throw new TypeError('Value must contain JSON data properties');
        if (isArray && (!/^(0|[1-9][0-9]*)$/.test(key) || Number(key) >= value.length)) throw new TypeError('Value must be a dense JSON array');
        result[key] = copy(field.value, depth + 1); count++;
      }
      if (isArray && count !== value.length) throw new TypeError('Value must be a dense JSON array');
      remove(seen, value); return result;
    }
    return stringify(copy(value, 0));
  }
  return { encode, parse, display(value) {
    return typeof value === 'object' && value !== null ? encode(value) : StringValue(value);
  }, error(value) {
    try { return StringValue(value && value.message !== undefined ? value.message : value); }
    catch { return 'Guest exception'; }
  }, rejection(Constructor, member, functionName, message) {
    const error = new Constructor(message); error[member] = functionName; return error;
  } };
})()`;

function jsonCopy(value: unknown, seen = new Set<object>(), depth = 0): PtcJsonValue {
  if (depth > 64) throw new TypeError('Binding value nesting exceeds 64');
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return value;
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value !== 'object' || seen.has(value)) throw new TypeError('Binding value must be lossless JSON');
  const array = Array.isArray(value), prototype = Object.getPrototypeOf(value);
  if (prototype !== null && prototype !== (array ? Array.prototype : Object.prototype)) throw new TypeError('Binding value must be plain JSON');
  seen.add(value);
  const fields = Object.getOwnPropertyDescriptors(value);
  const result: Record<string, PtcJsonValue> | PtcJsonValue[] = array ? [] : Object.create(null);
  let count = 0;
  for (const key of Reflect.ownKeys(fields)) {
    if (array && key === 'length') continue;
    const field = typeof key === 'string' ? fields[key] : undefined;
    if (!field || !('value' in field) || !field.enumerable) throw new TypeError('Binding value must contain JSON data properties');
    if (array) {
      if (typeof key !== 'string' || !/^(0|[1-9][0-9]*)$/u.test(key) || Number(key) >= value.length) throw new TypeError('Binding value must be a dense JSON array');
      Object.defineProperty(result, key, { value: jsonCopy(field.value, seen, depth + 1), enumerable: true, configurable: true, writable: true });
    } else Object.defineProperty(result, key, { value: jsonCopy(field.value, seen, depth + 1), enumerable: true, configurable: true, writable: true });
    count++;
  }
  if (array && count !== value.length) throw new TypeError('Binding value must be a dense JSON array');
  seen.delete(value); return result;
}

function validateBindings(bindings: PtcBindingNamespace[]) {
  const globals = new Set(['console']), classes = new Map<string, string>();
  const identifier = (name: string) => /^[A-Za-z_][A-Za-z0-9_]*$/u.test(name)
    && !PORTABLE_RESERVED_WORDS.has(name) && !RESERVED_BINDING_GLOBALS.has(name);
  for (const binding of bindings) {
    if (!identifier(binding.global) || globals.has(binding.global)) throw new TypeError('Invalid or duplicate binding namespace');
    globals.add(binding.global);
    for (const member of Object.values(binding.functions)) if (typeof member !== 'function') throw new TypeError('Binding members must be functions');
    const error = binding.errorClass;
    if (!error) continue;
    if (!identifier(error.name) || !error.memberNameProperty || RESERVED_ERROR_MEMBERS.has(error.memberNameProperty)
      || DUNDER_MEMBER.test(error.memberNameProperty) || classes.has(error.name) && classes.get(error.name) !== error.memberNameProperty) {
      throw new TypeError('Invalid binding rejection class');
    }
    classes.set(error.name, error.memberNameProperty);
  }
  for (const name of classes.keys()) if (globals.has(name)) throw new TypeError('Binding rejection class collides with namespace');
}

/** An ephemeral WASM guest. Only registered async JSON bindings cross into the host. */
export class InlinePtcRuntime extends PtcRuntime {
  static Config = Schema.object({ timeoutMs: integer(), maxTimeoutMs: integer(), cpuBudgetMs: integer(),
    memoryLimitBytes: integer(), maxStackSizeBytes: integer(), maxOutputBytes: integer(), maxBindingBytes: integer(),
    maxPendingCalls: integer(), maxConcurrentRuns: integer() });
  readonly language = 'typescript';
  readonly isolation = 'wasm';
  private readonly limits: Limits;
  private readonly active = new Set<{ abort: AbortController; done: Promise<void> }>();
  private closed = false;
  constructor(ctx: Context, config: Config = {}) {
    super(ctx);
    this.limits = { ...defaults, ...config };
    for (const value of Object.values(this.limits)) if (!Number.isSafeInteger(value) || value <= 0) throw new TypeError('Inline runtime limits must be positive integers');
    ctx.effect(() => async () => {
      this.closed = true;
      for (const run of this.active) run.abort.abort();
      await Promise.all([...this.active].map(run => run.done));
    });
  }
  get executionInstructions() {
    return 'Write an async TypeScript function body with await and return (erasable TypeScript only). Each call has fresh memory. Only the supplied JSON function bindings and console.log/info/warn/error are available. There is no import, Node, filesystem, network, process, timer or module access. Await every function call; return after registering a business wait. Only printed values and the returned JSON value become program output.';
  }
  get timeout() { return { defaultMs: Math.min(this.limits.timeoutMs, this.limits.maxTimeoutMs), maxMs: this.limits.maxTimeoutMs }; }
  resolve(request: PtcRunRequest): PtcRunSpec {
    if (this.closed) throw new Error('Inline runtime is disposed');
    if (request.sandboxPolicy !== undefined) throw new TypeError('Inline runtime has no filesystem authority or sandbox escalation');
    if (request.timeoutMs !== undefined && request.timeoutMs !== null && (!Number.isFinite(request.timeoutMs) || request.timeoutMs <= 0)) {
      throw new TypeError('Invalid inline runtime deadline');
    }
    validateBindings(request.bindings);
    return { ...request, cwd: request.cwd ?? '/', timeoutMs: request.timeoutMs === null ? null
      : Math.min(request.timeoutMs ?? this.timeout.defaultMs, this.timeout.maxMs) };
  }
  async release(_owner: object) { /* No retained state or pending guest process. */ }
  async run(spec: PtcRunSpec): Promise<PtcRunResult> {
    if (this.closed) throw new Error('Inline runtime is disposed');
    if (spec.sandboxPolicy !== undefined) throw new TypeError('Inline runtime has no filesystem authority');
    if (spec.timeoutMs !== null && (!Number.isFinite(spec.timeoutMs) || spec.timeoutMs <= 0 || spec.timeoutMs > this.timeout.maxMs)) throw new TypeError('Invalid resolved inline deadline');
    validateBindings(spec.bindings);
    if (this.active.size >= this.limits.maxConcurrentRuns) return { logs: [], error: { kind: 'worker-exit', message: 'Inline runtime concurrent run limit exceeded' } };
    const abort = new AbortController();
    let finish!: () => void;
    const done = new Promise<void>(resolve => { finish = resolve; });
    const run = { abort, done }; this.active.add(run);
    try { return await this.evaluate(spec, AbortSignal.any([abort.signal, ...(spec.signal ? [spec.signal] : [])])); }
    finally { this.active.delete(run); finish(); }
  }
  private async evaluate(spec: PtcRunSpec, signal: AbortSignal): Promise<PtcRunResult> {
    const logs: string[] = [];
    let failure: PtcRunFailure | undefined, stopped = false, outputBytes = 0;
    const deadline = spec.timeoutMs === null ? Infinity : Date.now() + spec.timeoutMs;
    let wake: (() => void) | undefined;
    const stop = (kind: PtcRunFailure['kind'], message: string) => { failure ??= { kind, message }; wake?.(); };
    const onAbort = () => stop('abort', 'Inline execution aborted');
    signal.addEventListener('abort', onAbort, { once: true });
    if (signal.aborted) onAbort();
    const timer = spec.timeoutMs === null ? undefined : setTimeout(() => stop('timeout', 'Inline execution deadline exceeded'), spec.timeoutMs);
    const admit = (text: string) => {
      if (outputBytes + Buffer.byteLength(text) > this.limits.maxOutputBytes) {
        stop('output-limit', 'Inline output limit exceeded'); return false;
      }
      outputBytes += Buffer.byteLength(text); return true;
    };
    let context: QuickJSContext | undefined, runtime: QuickJSRuntime | undefined, codec: QuickJSHandle | undefined, program: QuickJSHandle | undefined;
    const held = new Set<QuickJSHandle>(), deferred = new Set<QuickJSDeferredPromise>();
    const settlements: Array<() => void> = [];
    let cpuSpent = 0, cpuStart = 0, cpuDepth = 0;
    const checkBudget = () => {
      if (signal.aborted) onAbort();
      if (Date.now() >= deadline) stop('timeout', 'Inline execution deadline exceeded');
      if (cpuSpent + (cpuDepth ? performance.now() - cpuStart : 0) >= this.limits.cpuBudgetMs) stop('timeout', 'Inline CPU budget exceeded');
      return failure !== undefined;
    };
    const inGuest = <T>(action: () => T): T => {
      if (cpuDepth++ === 0) cpuStart = performance.now();
      try { return action(); }
      finally { if (--cpuDepth === 0) { cpuSpent += performance.now() - cpuStart; checkBudget(); } }
    };
    try {
      if (Buffer.byteLength(spec.program) > this.limits.maxBindingBytes) return { logs, error: { kind: 'protocol', message: 'Inline source limit exceeded' } };
      if (failure) return { logs, error: failure };
      const source = stripTypeScriptTypes(`(async () => {\n${spec.program}\n})()`, { mode: 'strip', sourceUrl: 'dsh-reactive-inline.ts' });
      // The single-file variant embeds WASM; no runtime asset path or guest loader is needed.
      const quickjs = await newQuickJSWASMModuleFromVariant(variant.default);
      runtime = quickjs.newRuntime();
      runtime.setMemoryLimit(this.limits.memoryLimitBytes);
      runtime.setMaxStackSize(this.limits.maxStackSizeBytes);
      runtime.setInterruptHandler(checkBudget);
      context = runtime.newContext();
      const vm = context;
      const unwrap = (result: ReturnType<QuickJSContext['evalCode']>): QuickJSHandle => {
        if (result.error) {
          try { throw new Error(errorText(result.error)); } finally { result.error.dispose(); }
        }
        return result.value;
      };
      const errorText = (error: QuickJSHandle) => {
        if (!codec || failure) return failure?.message ?? 'Guest exception';
        const helper = vm.getProp(codec, 'error');
        try {
          const result = inGuest(() => vm.callFunction(helper, vm.undefined, error));
          if (result.error) { result.error.dispose(); return 'Guest exception'; }
          return result.value.consume(value => vm.getString(value));
        } finally { helper.dispose(); }
      };
      codec = inGuest(() => unwrap(vm.evalCode(guestCodec, 'inline-codec.js')));
      const helper = (name: string) => { const value = vm.getProp(codec!, name); held.add(value); return value; };
      const encode = helper('encode'), parse = helper('parse'), display = helper('display'), rejection = helper('rejection');
      const readJson = (value: QuickJSHandle): string => {
        const text = inGuest(() => unwrap(vm.callFunction(encode, vm.undefined, value))).consume(handle => vm.getString(handle));
        if (Buffer.byteLength(text) > this.limits.maxBindingBytes) throw new TypeError('Binding argument limit exceeded');
        return text;
      };
      const fromJson = (value: unknown) => {
        const text = JSON.stringify(jsonCopy(value));
        if (Buffer.byteLength(text) > this.limits.maxBindingBytes) throw new TypeError('Binding result limit exceeded');
        const input = vm.newString(text);
        try { return inGuest(() => unwrap(vm.callFunction(parse, vm.undefined, input))); } finally { input.dispose(); }
      };
      const errors = new Map<string, QuickJSHandle>();
      for (const namespace of spec.bindings) {
        const error = namespace.errorClass;
        if (!error || errors.has(error.name)) continue;
        const constructor = inGuest(() => unwrap(vm.evalCode(`(class extends Error { constructor(message) { super(message); this.name = ${JSON.stringify(error.name)}; } })`, 'inline-error.js')));
        vm.setProp(vm.global, error.name, constructor);
        errors.set(error.name, constructor); held.add(constructor);
      }
      for (const namespace of spec.bindings) {
        const object = vm.newObject(vm.null);
        for (const [name, binding] of Object.entries(namespace.functions)) {
          const fn = vm.newFunction(name, (...args) => {
            if (stopped || failure) throw new Error('Inline run is over; binding not dispatched');
            if (args.length !== 1) throw new TypeError('Binding requires exactly one JSON argument');
            if (deferred.size >= this.limits.maxPendingCalls) throw new Error('Inline pending binding limit exceeded');
            const input: unknown = JSON.parse(readJson(args[0]!));
            const promise = vm.newPromise(); deferred.add(promise);
            const settle = (result: { value: unknown } | { error: unknown }) => {
              if (stopped) return;
              settlements.push(() => {
                let handle: QuickJSHandle | undefined;
                try {
                  if ('error' in result) {
                    const message = result.error instanceof Error ? result.error.message : 'Binding rejected';
                    const error = namespace.errorClass;
                    if (error) {
                      const values = [error.memberNameProperty, name, message].map(value => vm.newString(value));
                      try { handle = inGuest(() => unwrap(vm.callFunction(rejection, vm.undefined, errors.get(error.name)!, ...values))); }
                      finally { for (const value of values) value.dispose(); }
                    } else handle = vm.newError(message);
                    promise.reject(handle);
                  } else { handle = fromJson(result.value); promise.resolve(handle); }
                } catch (error) {
                  handle?.dispose(); handle = vm.newError(error instanceof Error ? error.message : 'Invalid binding result'); promise.reject(handle);
                } finally { handle?.dispose(); deferred.delete(promise); promise.dispose(); }
              });
              wake?.();
            };
            void Promise.resolve().then(() => {
              if (stopped || failure || signal.aborted) throw new Error('Inline run is over; binding not dispatched');
              return binding(input);
            }).then(value => settle({ value }), error => settle({ error }));
            return promise.handle.dup();
          });
          vm.setProp(object, name, fn); fn.dispose();
        }
        vm.setProp(vm.global, namespace.global, object); object.dispose();
      }
      const consoleObject = vm.newObject(vm.null);
      for (const name of ['log', 'info', 'warn', 'error']) {
        const fn = vm.newFunction(name, (...args) => {
          const text = args.map(arg => inGuest(() => unwrap(vm.callFunction(display, vm.undefined, arg))).consume(value => vm.getString(value))).join(' ');
          if (admit(text + '\n')) logs.push(text);
        });
        vm.setProp(consoleObject, name, fn); fn.dispose();
      }
      vm.setProp(vm.global, 'console', consoleObject); consoleObject.dispose();
      program = inGuest(() => unwrap(vm.evalCode(source, 'dsh-reactive-inline.js')));
      while (!failure) {
        for (const settle of settlements.splice(0)) inGuest(settle);
        const state = inGuest(() => vm.getPromiseState(program!));
        if (state.type === 'rejected') {
          try { stop('exception', errorText(state.error)); } finally { state.error.dispose(); }
          break;
        }
        if (state.type === 'fulfilled') {
          try {
            if (vm.typeof(state.value) === 'undefined') return { logs };
            const text = readJson(state.value);
            if (!admit(text)) break;
            const value: PtcJsonValue = JSON.parse(text); return { logs, value };
          } catch (error) { stop('invalid-output', error instanceof Error ? error.message : 'Invalid inline result'); }
          finally { state.value.dispose(); }
          break;
        }
        const jobs = inGuest(() => runtime!.executePendingJobs(32));
        if (jobs.error) { try { stop('exception', errorText(jobs.error)); } finally { jobs.error.dispose(); } break; }
        if (failure) break;
        if (jobs.value > 0 || settlements.length) await yieldToHost();
        else await new Promise<void>(resolve => { wake = resolve; if (settlements.length || failure) resolve(); }).finally(() => { wake = undefined; });
      }
      return { logs, error: failure! };
    } catch (error) {
      return { logs, error: failure ?? { kind: 'exception', message: error instanceof Error ? error.message : 'Inline execution failed' } };
    } finally {
      stopped = true; clearTimeout(timer); signal.removeEventListener('abort', onAbort);
      for (const promise of deferred) promise.dispose();
      program?.dispose(); for (const value of held) value.dispose(); codec?.dispose();
      context?.dispose(); runtime?.dispose();
    }
  }
}
export default InlinePtcRuntime;
