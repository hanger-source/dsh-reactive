import type { Observable } from 'rxjs';
import type { z } from 'zod';
import type { Scope } from '../reactivity/scope.ts';
import { connect } from '../reactivity/observable.ts';
import { bindResource, type ResourceInstance } from '../resources/binding.ts';
import type { ResourceContract, ResourceParameters } from '../resources/definition.ts';
import { isResourceImplementation, type ResourceImplementation } from '../resources/implementation.ts';
import { createResource, type LocalResource, type LocalResourceOptions } from '../resources/local.ts';
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
  /** 正式实现与解析参数稳定共享同一实例。 */
  resource<D extends ResourceContract>(implementation: ResourceImplementation<D>, params: ResourceParameters<D>): ResourceInstance<D>;
  /** 每次构造独立的局部实例；调用者显式保留或共享返回对象，构造不读取。 */
  resource<V extends z.ZodType>(options: LocalResourceOptions<V>): LocalResource<V>;
  reaction<D extends ReactionContract>(implementation: ReactionImplementation<D>, params: ReactionParameters<D>): ReactionInstance<D>;
  /** 局部实例归工作区所有；每次调用独立构造，start仍是显式执行入口。 */
  reaction<E extends z.ZodType, R extends z.ZodType>(options: LocalReactionOptions<E, R>): LocalReaction<E, R>;
}

/** @internal 内核装配持有Scope；业务和Program借用Workspace，不释放整个工作区。 */
export interface WorkspaceContext extends Workspace { readonly scope: Scope; }

/** @internal 实例均归实际工作区Scope；正式实现共享参数身份，局部选项每次构造新身份。 */
export function createWorkspaceContext(scope: Scope, identity: { readonly id: string; readonly directory: string }): WorkspaceContext {
  scope.assertActive();
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
  return Object.freeze({ ...identity, scope,
    share: <T>(source: Observable<T>) => connect(scope, source),
    resource, reaction,
  });
}
