import type { Observable } from 'rxjs';
import type { Scope } from '../reactivity/scope.ts';
import { connect } from '../reactivity/observable.ts';
import { bindResource, type ResourceInstance } from '../resources/binding.ts';
import type { ResourceContract, ResourceParameters } from '../resources/definition.ts';
import type { ResourceImplementation } from '../resources/implementation.ts';
import { bindReaction, type ReactionInstance } from '../reactions/binding.ts';
import type { ReactionContract, ReactionParameters } from '../reactions/definition.ts';
import type { ReactionImplementation } from '../reactions/implementation.ts';

/** 本地工作区的编程接点；登记与作用域由宿主拥有，业务显式绑定资源及反应。 */
export interface Workspace {
  readonly id: string;
  readonly directory: string;
  /** 立即共享来源，消费者退订不停止工作区持有的连接。 */
  share<T>(source: Observable<T>): Observable<T>;
  resource<D extends ResourceContract>(implementation: ResourceImplementation<D>, params: ResourceParameters<D>): ResourceInstance<D>;
  reaction<D extends ReactionContract>(implementation: ReactionImplementation<D>, params: ReactionParameters<D>): ReactionInstance<D>;
}

/** @internal 内核装配持有Scope；业务和Program借用Workspace，不释放整个工作区。 */
export interface WorkspaceContext extends Workspace { readonly scope: Scope; }

/** @internal 同一Scope沿已有绑定缓存持有身份，不建立第二份资源或反应状态。 */
export function createWorkspaceContext(scope: Scope, identity: { readonly id: string; readonly directory: string }): WorkspaceContext {
  scope.assertActive();
  return Object.freeze({ ...identity, scope,
    share: <T>(source: Observable<T>) => connect(scope, source),
    resource: <D extends ResourceContract>(implementation: ResourceImplementation<D>, params: ResourceParameters<D>) =>
      bindResource(implementation, { scope }).instance(params),
    reaction: <D extends ReactionContract>(implementation: ReactionImplementation<D>, params: ReactionParameters<D>) =>
      bindReaction(implementation, { scope }).instance(params),
  });
}
