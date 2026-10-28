import { UnsubscriptionError, type Observable } from 'rxjs';
import type { z } from 'zod';
import type { Scope } from '../reactivity/scope.ts';
import { connect } from '../reactivity/observable.ts';
import { bindResource, type ResourceInstance } from '../resources/binding.ts';
import type { ResourceContract, ResourceParameters } from '../resources/definition.ts';
import { isResourceImplementation, type ResourceImplementation } from '../resources/implementation.ts';
import { createResource, type LocalResource, type LocalResourceOptions } from '../resources/local.ts';
import { shareResource } from '../resources/sharing.ts';
import { resourceObservations, type ResourceSource } from '../resources/source.ts';
import type { ResourceReader } from '../resources/reader.ts';
import { bindReaction, type ReactionInstance } from '../reactions/binding.ts';
import type { ReactionContract, ReactionParameters } from '../reactions/definition.ts';
import { isReactionImplementation, type ReactionImplementation } from '../reactions/implementation.ts';
import { createReaction, type LocalReaction, type LocalReactionOptions } from '../reactions/local.ts';

/** 本地工作区的编程接点；登记与作用域由宿主拥有，业务显式绑定资源及反应。 */
export interface Workspace {
  readonly id: string;
  readonly directory: string;
  /** 立即共享来源，消费者退订不停止工作区持有的连接。 */
  share<T>(source: Observable<T>): Observable<T>;
  /** 同一资源观察流共享读侧，工作区退出撤销有效性；构造不连接，最后消费者退出释放来源。 */
  shareResource<T>(source: ResourceSource<T>): ResourceReader<T>;
  /** 正式实现与解析参数稳定共享同一实例。 */
  resource<D extends ResourceContract>(implementation: ResourceImplementation<D>, params: ResourceParameters<D>): ResourceInstance<D>;
  /** 每次构造独立的局部实例；调用者显式保留或共享返回对象，构造不读取。 */
  resource<V extends z.ZodType>(options: LocalResourceOptions<V>): LocalResource<V>;
  reaction<D extends ReactionContract>(implementation: ReactionImplementation<D>, params: ReactionParameters<D>): ReactionInstance<D>;
  /** 局部实例归工作区所有；每次调用独立构造，start仍是显式执行入口。 */
  reaction<E extends z.ZodType, R extends z.ZodType>(options: LocalReactionOptions<E, R>): LocalReaction<E, R>;
}

/** @internal 实例均归实际工作区Scope；正式实现共享参数身份，局部选项每次构造新身份。 */
export function createWorkspace(scope: Scope, identity: { readonly id: string; readonly directory: string }): Workspace {
  scope.assertActive();
  const resources = new WeakMap<Observable<unknown>, unknown>();
  const cleanupFailures: unknown[] = [];
  const recordCleanupFailure = (error: unknown): void => {
    // 只拆RxJS的释放包装，业务AggregateError仍是原来的一个失败事实。
    if (error instanceof UnsubscriptionError && error.errors.length) {
      for (const failure of error.errors) recordCleanupFailure(failure);
    } else cleanupFailures.push(error);
  };
  scope.own(() => {
    if (cleanupFailures.length === 1) throw cleanupFailures[0];
    if (cleanupFailures.length) throw new AggregateError(cleanupFailures, 'REACTIVE_WORKSPACE_RESOURCE_CLEANUP_FAILED');
  });
  function sharedResource<T>(source: ResourceSource<T>): ResourceReader<T> {
    scope.assertActive();
    const observations$ = resourceObservations(source);
    const previous = resources.get(observations$);
    // 同一个原观察流对应同一个T；身份不从资源名称或公开字段重新推断。
    if (previous) return previous as ResourceReader<T>;
    const reader = shareResource(observations$, { signal: scope.signal, onObserverError: error => scope.observerError(error),
      onCleanupError: recordCleanupFailure });
    resources.set(observations$, reader);
    return reader;
  }
  function resource<D extends ResourceContract>(implementation: ResourceImplementation<D>, params: ResourceParameters<D>): ResourceInstance<D>;
  function resource<V extends z.ZodType>(options: LocalResourceOptions<V>): LocalResource<V>;
  function resource(source: ResourceImplementation | LocalResourceOptions<z.ZodType>, params?: unknown): ResourceInstance<ResourceContract> {
    return isResourceImplementation(source)
      ? bindResource(source, { scope }).instance(params)
      : createResource(scope, source);
  }
  function reaction<D extends ReactionContract>(implementation: ReactionImplementation<D>, params: ReactionParameters<D>): ReactionInstance<D>;
  function reaction<E extends z.ZodType, R extends z.ZodType>(options: LocalReactionOptions<E, R>): LocalReaction<E, R>;
  function reaction(source: ReactionImplementation | LocalReactionOptions<z.ZodType, z.ZodType>, params?: unknown): ReactionInstance<ReactionContract> {
    return isReactionImplementation(source)
      ? bindReaction(source, { scope }).instance(params)
      : createReaction(scope, source);
  }
  return Object.freeze({ id: identity.id, directory: identity.directory,
    share: <T>(source: Observable<T>) => connect(scope, source),
    shareResource: sharedResource,
    resource, reaction,
  });
}
