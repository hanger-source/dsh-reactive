import type { ReadonlySignal } from '@preact/signals-core';
import { Value, type Scope } from '../reactivity/scope.ts';
import type { Observation } from './observation.ts';
import { ResourceReader } from './reader.ts';

type Node<T> = Pick<ReadonlySignal<T>, 'value' | 'peek' | 'subscribe'>;

export class ResourceValue<T> extends ResourceReader<T> {
  #observation: Value<Observation<T>>;
  constructor(scope: Scope, node: Node<Observation<T>>) {
    const observation = new Value(scope, node);
    super({ observations$: observation.changes$, peek: () => observation.peek() },
      { signal: scope.signal, onObserverError: error => scope.observerError(error) });
    this.#observation = observation;
  }
  get value() { return this.#observation.value; }
}
