import * as Cordis from '@deepseek-ai/cordis';
import * as Store from '@deepseek-ai/dsh-client-store';
import * as Zod from 'zod';
import commands from '@deepseek-ai/dsh-commands/remote';
import controller from '@deepseek-ai/dsh-api-session-controller/remote';
import uploads from '@deepseek-ai/dsh-client-file-upload/remote';
import subagents from '@deepseek-ai/dsh-subagent/remote';
import { SessionId } from '@deepseek-ai/dsh-session/types';
import { bindProgramClient, createAuthenticatedTransport } from '@hanger-source/dsh-reactive-client';
import remotes from '@hanger-source/dsh-reactive-client/remote';
import { retainNativeSession } from '@hanger-source/dsh-reactive-native-ui';
import { program } from './browser-contracts.mjs';

const output = document.querySelector('output');
const check = (condition, label) => { if (!condition) throw new Error(label); };
const rejects = async (pending, predicate) => {
  try { await pending; } catch (error) { check(predicate(error), 'UNEXPECTED_FAILURE:' + error); return; }
  throw new Error('EXPECTED_FAILURE');
};
const modules = window.__ModuleLoader__.create({ boot: window.__DSH_BOOT__, staticModules: {
  '@deepseek-ai/cordis': Cordis, '@deepseek-ai/dsh-client-store': Store, zod: Zod,
} });
const Registry = await modules.import('@deepseek-ai/dsh-typert-registry/client', location.href, {});
const Gateway = await modules.import('@deepseek-ai/dsh-api-gateway/client', location.href, {});
const { installConnection } = await modules.import('@deepseek-ai/dsh-client-connection/client', location.href, {});
const FileUpload = await modules.import('@deepseek-ai/dsh-client-file-upload/client', location.href, {});
const ClientSessions = await modules.import('@deepseek-ai/dsh-api-session-controller/client', location.href, {});
document.querySelector('button').addEventListener('click', async event => {
  event.currentTarget.disabled = true;
  const ctx = new Cordis.Context(), leases = [];
  let delayed, sendsAfterClose = 0;
  const delayNextOpen = () => delayed = { received: Promise.withResolvers(), release: Promise.withResolvers(), entered: false };
  try {
    await ctx.plugin(Registry);
    installConnection(ctx, { interactions: 'answer', transport: createAuthenticatedTransport({ baseUrl: location.origin,
      createWebSocket(url) {
        const socket = new WebSocket(url), send = socket.send;
        socket.send = function (data) {
          if (socket.readyState !== WebSocket.OPEN) sendsAfterClose++;
          return send.call(socket, data);
        };
        return socket;
      },
      async fetch(url, init) {
        const response = await fetch(url, init);
        const gate = delayed;
        if (gate && !gate.entered && new URL(url).pathname.endsWith('/reactivePrograms/open')) {
          gate.entered = true;
          const reply = await response.clone().json();
          check(reply.result.ok, 'REAL_OPEN_MUST_BE_ACCEPTED');
          gate.received.resolve(); await gate.release.promise;
        }
        return response;
      },
    }) });
    await ctx.plugin(Gateway);
    for (const contribution of [commands, controller, uploads, subagents, remotes]) await ctx.remote.$mount(contribution);
    await ctx.plugin(FileUpload); await ctx.plugin(ClientSessions);
    const sdk = bindProgramClient(program, ctx);
    const { workspaceId, sessionId } = await fetch('/setup').then(response => response.json());
    const created = await sdk.create('work', { workspaceId, sessionId }); await created.release();
    const counts = id => ctx.sessions.retainInfo(SessionId(id)).getSnapshot();
    check(counts(sessionId).referenceCount === 0, 'INITIAL_NATIVE_REFERENCES');
    const first = await retainNativeSession(program, ctx, sessionId); leases.push(first);
    await rejects(retainNativeSession(program, first.session.native.ctx, sessionId),
      error => String(error).includes('REACTIVE_NATIVE_SESSION_REFERENCE_OWNER_REQUIRED'));
    check(counts(sessionId).referenceCount === 1, 'GENERATION_CANNOT_RETAIN_ITS_OWN_LIFETIME');
    const acquisition = new AbortController();
    const second = await retainNativeSession(program, ctx, sessionId, { signal: acquisition.signal }); leases.push(second);
    acquisition.abort(new Error('AFTER_ACQUISITION'));
    check(first.session.native === second.session.native, 'RETAIN_SAME_NATIVE_GENERATION');
    check(counts(sessionId).referenceCount === 2 && counts(sessionId).retainedBy.reactiveProgram === 2, 'TWO_EXACT_NATIVE_REFERENCES');
    check(await first.session.readView('current') instanceof Date, 'NATIVE_RESOURCE_DECODES_DATE');
    let notifications = 0;
    const view = first.session.view('current');
    const stop = view.subscribe(() => notifications++);
    await first.session.readView('current');
    await Promise.all([first.release(), first.release()]);
    const frozen = notifications;
    check(counts(sessionId).referenceCount === 1, 'RELEASE_ONLY_THIS_REFERENCE');
    await rejects(Promise.resolve().then(() => first.session.readView('current')), error => String(error).includes('REACTIVE_SCOPE_DISPOSED'));
    const action = await second.session.resolveAction('check', null);
    await (await action({ amount: '7' })).result;
    check((await second.session.readView('current')).toISOString() === '2026-10-26T00:00:07.000Z', 'PEER_REMAINS_LIVE');
    check(notifications === frozen, 'RELEASED_UI_OBSERVER_STOPPED');
    stop();

    const gate = delayNextOpen(), cancelled = new AbortController(), reason = new Error('CANCEL_NATIVE_ACQUISITION');
    const rejected = rejects(retainNativeSession(program, ctx, sessionId, { signal: cancelled.signal }), error => error === reason);
    await gate.received.promise; cancelled.abort(reason); await rejected;
    check(counts(sessionId).referenceCount === 1, 'CANCELLED_ACQUISITION_RELEASES_REFERENCE');
    gate.release.resolve(); delayed = undefined;
    const absent = crypto.randomUUID();
    await rejects(retainNativeSession(program, ctx, absent), error => error instanceof Error);
    check(counts(absent).referenceCount === 0, 'FAILED_ACQUISITION_RELEASES_REFERENCE');
    const oldBinding = second.session.native;
    await second.release();
    check(counts(sessionId).referenceCount === 0 && !ctx.sessions.binding(SessionId(sessionId)), 'LAST_REFERENCE_RELEASES_NATIVE_GENERATION');
    const reopened = await retainNativeSession(program, ctx, sessionId); leases.push(reopened);
    check(reopened.session.native !== oldBinding, 'NEW_NATIVE_GENERATION');
    check(await reopened.session.readView('current') instanceof Date, 'REOPENED_HOST_FACT_REMAINS');
    const finalGate = delayNextOpen();
    const ended = rejects(retainNativeSession(program, ctx, sessionId), error => String(error).includes('REFERENCE_RELEASED'));
    await finalGate.received.promise; await ctx.fiber.dispose(); await ended;
    finalGate.release.resolve(); delayed = undefined;
    check(reopened.session.scope.disposed, 'CONTEXT_RELEASES_OWNED_UI');
    const proof = await fetch('/proof').then(response => response.json());
    check(proof.executions === 1 && proof.models === 0, 'NO_EXTRA_BUSINESS_OR_MODEL_EXECUTION');
    check(sendsAfterClose === 0, 'NO_SEND_AFTER_PHYSICAL_SOCKET_CLOSE:' + sendsAfterClose);
    output.textContent = JSON.stringify({ passed: true, sessionId, models: proof.models, executions: proof.executions,
      sameGeneration: true, independentRelease: true, cancelledAcquisition: true, failedAcquisition: true, reopenedGeneration: true, contextClosedPending: true, sendsAfterClose });
  } catch (error) { output.textContent = JSON.stringify({ passed: false, error: String(error), stack: error.stack }); }
  finally { delayed?.release.resolve(); await Promise.all(leases.map(lease => lease.release())); await ctx.fiber.dispose(); }
});
output.textContent = '已装配正式 rc.3 Client Sessions，可验证引用归属';
