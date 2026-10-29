import * as Cordis from '@deepseek-ai/cordis';
import * as Store from '@deepseek-ai/dsh-client-store';
import * as React from 'react';
import * as JSXRuntime from 'react/jsx-runtime';
import * as ReactDOM from 'react-dom';
import * as ReactDOMClient from 'react-dom/client';
import * as Slots from '@deepseek-ai/dsh-client-ui-slots';
import * as Zod from 'zod';
import { filter, firstValueFrom, timeout } from 'rxjs';
import commands from '@deepseek-ai/dsh-commands/remote';
import controller from '@deepseek-ai/dsh-api-session-controller/remote';
import uploads from '@deepseek-ai/dsh-client-file-upload/remote';
import subagents from '@deepseek-ai/dsh-subagent/remote';
import { SessionId } from '@deepseek-ai/dsh-session/types';
import { bindProgramClient, createAuthenticatedTransport } from '@hanger-source/dsh-reactive-client';
import remotes from '@hanger-source/dsh-reactive-client/remote';
import { bindNativeSession, nativeHooks, retainNativeSession } from '@hanger-source/dsh-reactive-native-ui';
import { nativeProgram as program, layout, rowKey } from './browser-contracts.mjs';

const output = document.querySelector('output');
const container = document.querySelector('#native-renderer');
const check = (condition, label) => { if (!condition) throw new Error(label); };
const rejects = async (pending, predicate) => {
  try { await pending; } catch (error) { check(predicate(error), 'UNEXPECTED_FAILURE:' + error); return; }
  throw new Error('EXPECTED_FAILURE');
};
const next = (reader, predicate) => firstValueFrom(reader.observations$.pipe(filter(predicate), timeout(8000)));
const rendered = (predicate, label) => new Promise((resolve, reject) => {
  const accept = () => {
    if (!predicate()) return false;
    observer.disconnect(); clearTimeout(deadline); resolve(); return true;
  };
  const observer = new MutationObserver(accept);
  const deadline = setTimeout(() => { observer.disconnect(); reject(new Error(label + ':' + container.textContent)); }, 8000);
  observer.observe(container, { subtree: true, childList: true, characterData: true });
  accept();
});
const modules = window.__ModuleLoader__.create({ boot: window.__DSH_BOOT__, staticModules: {
  '@deepseek-ai/cordis': Cordis, '@deepseek-ai/dsh-client-store': Store, zod: Zod,
  react: React, 'react/jsx-runtime': JSXRuntime, 'react-dom': ReactDOM, 'react-dom/client': ReactDOMClient,
  '@deepseek-ai/dsh-client-ui-slots': Slots,
} });
const Registry = await modules.import('@deepseek-ai/dsh-typert-registry/client', location.href, {});
const Gateway = await modules.import('@deepseek-ai/dsh-api-gateway/client', location.href, {});
const { installConnection } = await modules.import('@deepseek-ai/dsh-client-connection/client', location.href, {});
const FileUpload = await modules.import('@deepseek-ai/dsh-client-file-upload/client', location.href, {});
const ClientSessions = await modules.import('@deepseek-ai/dsh-api-session-controller/client', location.href, {});
const Renderer = await modules.import('@deepseek-ai/dsh-client-ui-renderer/client', location.href, {});
const RemoteAccess = await modules.import('@deepseek-ai/dsh-api-remote-access/client', location.href, {});
const UiSession = await modules.import('@deepseek-ai/dsh-client-ui-session/client', location.href, {});
document.querySelector('button').addEventListener('click', async event => {
  event.currentTarget.disabled = true;
  const ctx = new Cordis.Context(), leases = [];
  let delayed, sendsAfterClose = 0, mounted = 0, unmounted = 0, releaseView, actionHeld = false, borrowed, borrowedUi, transport, result;
  const failures = [];
  const cleanup = async dispose => { try { await dispose(); } catch (error) { failures.push(error); } };
  const delayNextOpen = () => delayed = { received: Promise.withResolvers(), release: Promise.withResolvers(), entered: false };
  const mountView = async session => {
    const action = await session.resolveAction('root.check', null);
    return ctx.effect(() => {
    const remove = ctx.slots.register({ name: 'root', inject: () => nativeHooks({ current: session.module(layout.mounts.root).viewResource('current'),
      record: session.module(layout.mounts.overview).viewResource('record'), eligibility: action.eligibility,
      file: session.resource(session.files.text('native.txt')) }) }, function NativeCurrent({ useCurrent, useFile, useRecord, useEligibility }) {
      const current = useCurrent(value => value);
      const file = useFile(value => value);
      const record = useRecord(value => value), eligibility = useEligibility(value => value);
      React.useLayoutEffect(() => { mounted++; return () => { unmounted++; }; }, []);
      return React.createElement('section', { 'aria-label': '原生资源观察' },
        React.createElement('time', { 'data-native-current': true }, current.status === 'ready' ? current.value.toISOString() : current.status),
        React.createElement('p', { 'data-native-file': true }, file.status === 'ready' ? file.value.text : file.status),
        React.createElement('p', { 'data-native-record': true }, record.status === 'ready' ? record.value : record.status),
        React.createElement('p', { 'data-native-eligibility': true }, eligibility.status === 'ready'
          ? eligibility.value.allowed ? 'allowed' : eligibility.value.reason : eligibility.status));
    });
    try {
      const unmount = ctx.uiRenderer.mount(container);
      return () => { unmount(); remove(); };
    } catch (error) { remove(); throw error; }
    }, 'native fixture root Slot and renderer');
  };
  const nativeGate = async action => {
    const response = await fetch('/native-action/' + action, { method: action === 'entered' ? 'GET' : 'POST', signal: AbortSignal.timeout(8000) });
    check(response.ok, 'NATIVE_ACTION_GATE:' + action + ':' + response.status);
  };
  const surfaceControl = async action => {
    const response = await fetch('/native-surface/' + action, { method: 'POST', signal: AbortSignal.timeout(8000) });
    check(response.ok, 'NATIVE_SURFACE_CONTROL:' + action + ':' + response.status);
  };
  try {
    check(React.version === '18.3.1' && ReactDOM.version.startsWith('18.3.1'), 'NATIVE_REACT_18_PLATFORM_REQUIRED');
    await ctx.plugin(Registry);
    transport = createAuthenticatedTransport({ baseUrl: location.origin,
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
    });
    ctx.effect(() => () => transport.dispose(), 'authenticated transport');
    installConnection(ctx, { interactions: 'answer', transport });
    await ctx.plugin(Gateway);
    for (const contribution of [commands, controller, uploads, subagents, remotes]) await ctx.remote.$mount(contribution);
    await ctx.plugin(FileUpload); await ctx.plugin(ClientSessions);
    await ctx.plugin(Renderer);
    await ctx.plugin(RemoteAccess); await ctx.plugin(UiSession);
    const sdk = await bindProgramClient(program, ctx);
    const { workspaceId, nativeSessionId: sessionId } = await fetch('/setup').then(response => response.json());
    const created = await sdk.create('native-work', { workspaceId, sessionId }); await created.release();
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
    check(await first.session.module(layout.mounts.root).readView('current') instanceof Date, 'NATIVE_RESOURCE_DECODES_DATE');
    borrowed = await sdk.open(sessionId);
    borrowedUi = bindNativeSession(program, first.session.native, borrowed);
    const overview = borrowedUi.module(layout.mounts.overview), originalOverview = borrowed.module(layout.mounts.overview);
    const rows = borrowedUi.collection('rows'), row = rows.instance(rowKey);
    const record = overview.viewResource('record'), directory = rows.instances;
    check((await directory.read()).join() === rowKey, 'NATIVE_COLLECTION_REAL_DIRECTORY');
    check(await record.read() === '原生模块第一份', 'NATIVE_STATIC_MODULE_REAL_RESOURCE');
    check(record.peek() === originalOverview.viewResource('record').peek(), 'NATIVE_MODULE_BORROWS_SAME_RESOURCE_FACT');
    check(borrowedUi.module(layout.mounts.overview).viewResource('record') === record, 'NATIVE_MODULE_REUSES_RESOURCE_READER');
    check(await row.readView('record') === '原生模块第一份', 'NATIVE_DYNAMIC_MODULE_RESOURCE');
    const saved = await row.resolveAction('save', rowKey + ':1');
    const qualified = saved.eligibility;
    check(qualified && (await qualified.read()).allowed, 'NATIVE_MODULE_ACTION_ELIGIBILITY');
    const sdkSaved = await borrowed.collection('rows').instance(rowKey).resolveAction('save', rowKey + ':1');
    check(qualified.peek() === sdkSaved.eligibility.peek(), 'NATIVE_ACTION_BORROWS_SAME_ELIGIBILITY_FACT');
    await surfaceControl('pending');
    await next(qualified, value => value.status === 'pending');
    await surfaceControl('denied');
    const denied = await next(qualified, value => value.status === 'ready' && !value.value.allowed);
    check(denied.value.reason === '原生资料尚未确认', 'NATIVE_ELIGIBILITY_PRESERVES_DENIAL_REASON');
    await rejects(saved('forbidden').then(invocation => invocation.result), error => error.reason === 'denied'
      && error.observation.value.reason === '原生资料尚未确认');
    await surfaceControl('allowed');
    await next(qualified, value => value.status === 'ready' && value.value.allowed);
    const oldOperation = await row.operations.current();
    const originalSdkOperation = await borrowed.collection('rows').instance(rowKey).operations.current();
    check((await borrowedUi.operations['overview.current']()).command === 'overview:1', 'NATIVE_ROOT_OPERATION_SLOT');
    check(oldOperation.command === rowKey + ':1', 'NATIVE_OPERATION_CAPTURES_EXACT_COMMAND');
    await nativeGate('hold'); actionHeld = true;
    const moduleAccepted = await saved('accepted-old-generation');
    const acceptedResult = moduleAccepted.result;
    await nativeGate('entered');
    const moduleLookup = await row.invocation('save', moduleAccepted.id);
    check(moduleLookup.id === moduleAccepted.id, 'NATIVE_MODULE_LOOKUP_REUSES_ACCEPTED_INVOCATION');
    await surfaceControl('remove');
    await next(directory, value => value.status === 'ready' && value.value.length === 0);
    await rejects(saved('withdrawn').then(invocation => invocation.result), error => /BINDING_UNAVAILABLE|binding-unavailable/u.test(String(error) + error.code));
    const withdrawnLookup = await row.invocation('save', moduleAccepted.id);
    check(withdrawnLookup.id === moduleAccepted.id, 'WITHDRAWN_MODULE_LOOKUP_RETAINS_ORIGINAL_INVOCATION');
    await surfaceControl('replace');
    await next(directory, value => value.status === 'ready' && value.value.includes(rowKey));
    const replacement = await row.resolveAction('save', rowKey + ':2');
    check(replacement.bindingId !== saved.bindingId, 'SAME_KEY_REMOUNT_HAS_NEW_ACTION_BINDING');
    check((await row.operations.current()).command === rowKey + ':2' && oldOperation.command === rowKey + ':1', 'REMOUNT_DOES_NOT_RETARGET_CAPTURED_OPERATION');
    await rejects(saved('stale').then(invocation => invocation.result), error => /BINDING_UNAVAILABLE|binding-unavailable/u.test(String(error) + error.code));
    const originalPage = borrowed.files.text('native.txt'), uiPage = borrowedUi.files.text('native.txt');
    check(borrowedUi.files.text('native.txt') === uiPage, 'UI_REUSES_ONE_READER_FOR_THE_SAME_SDK_RESOURCE');
    const observedFile = [];
    const stopFile = uiPage.observations$.subscribe(observation => observedFile.push(observation));
    check((await uiPage.read()).text === '原生文件第一份', 'UI_FILE_READ_USES_REAL_HOST_CONTENT');
    check(uiPage.peek() === originalPage.peek(), 'UI_BORROWS_ORIGINAL_FILE_OBSERVATION');
    const watch = borrowedUi.files.watch('native.txt');
    await firstValueFrom(watch.changes$.pipe(filter(frame => frame.kind === 'ready'), timeout(8000)));
    let watchClosed = false;
    const watchClosing = watch.closed.then(() => { watchClosed = true; });
    const closingUi = borrowedUi.dispose();
    check(borrowedUi.scope.disposed && borrowedUi.dispose() === closingUi, 'UI_REVOKES_IMMEDIATELY_AND_JOINS_ONE_CLOSE');
    try { uiPage.peek(); throw new Error('EXITED_UI_FILE_MUST_REJECT'); }
    catch (error) { check(error === borrowedUi.scope.signal.reason, 'UI_FILE_READER_RETAINS_EXACT_EXIT_REASON'); }
    for (const source of [record, directory, qualified]) {
      try { source.peek(); throw new Error('EXITED_MODULE_READER_MUST_REJECT'); }
      catch (error) { check(error === borrowedUi.scope.signal.reason, 'MODULE_READER_RETAINS_EXACT_UI_EXIT_REASON'); }
    }
    await rejects(Promise.resolve().then(() => replacement('exited')), error => error === borrowedUi.scope.signal.reason);
    await rejects(Promise.resolve().then(() => oldOperation.locate()), error => error === borrowedUi.scope.signal.reason);
    await nativeGate('release'); actionHeld = false;
    check(moduleAccepted.result === acceptedResult && await acceptedResult === rowKey + ':1'
      && await moduleLookup.result === rowKey + ':1' && await withdrawnLookup.result === rowKey + ':1', 'UI_DISPOSE_PRESERVES_ACCEPTED_MODULE_RESULT');
    const locatedOriginal = await originalSdkOperation.locate();
    check(locatedOriginal.status === 'accepted' && locatedOriginal.reference === rowKey + ':1', 'CAPTURED_COMMAND_LOCATES_REAL_OLD_GENERATION_EFFECT');
    const externalOperation = await borrowed.collection('rows').instance(rowKey).operations.current();
    const originalOperation = await moduleAccepted.operation;
    check((await originalOperation.settled()).phase === 'done' && originalOperation.reference === rowKey + ':1', 'ACCEPTED_OPERATION_RETAINS_ORIGINAL_COMMAND_AFTER_UI_EXIT');
    const outside = await originalOverview.resolveAction('save', 'overview:1');
    check(await (await outside('external-sdk-survives')).result === 'overview:1', 'EXTERNAL_SDK_ACTION_SURVIVES_UI_DISPOSE');
    check((await externalOperation.locate()).status === 'unknown', 'EXTERNAL_SDK_OPERATION_REMAINS_USABLE');
    await closingUi; await watchClosing;
    check(watchClosed && observedFile.at(-1).status === 'error', 'UI_CLOSE_JOINS_OWNED_WATCH_AND_ENDS_ITS_READER');
    stopFile.unsubscribe();
    check((await borrowed.files.readText('native.txt')).text === '原生文件第一份', 'UI_CLOSE_PRESERVES_BORROWED_SDK');
    await borrowed.release(); borrowed = undefined; borrowedUi = undefined;
    let notifications = 0;
    const view = first.session.source(first.session.module(layout.mounts.root).viewResource('current'));
    const stop = view.subscribe(() => notifications++);
    await first.session.module(layout.mounts.root).readView('current');
    await Promise.all([first.release(), first.release()]);
    const frozen = notifications;
    check(counts(sessionId).referenceCount === 1, 'RELEASE_ONLY_THIS_REFERENCE');
    await rejects(Promise.resolve().then(() => first.session.module(layout.mounts.root).readView('current')), error => String(error).includes('REACTIVE_SCOPE_DISPOSED'));
    releaseView = await mountView(second.session);
    await rendered(() => container.querySelector('time')?.textContent === '2026-10-26T00:00:00.000Z', 'NATIVE_ROOT_RESOURCE_BASELINE');
    await rendered(() => container.querySelector('[data-native-file]')?.textContent === '原生文件第一份', 'NATIVE_FILE_SLOT_BASELINE');
    await rendered(() => container.querySelector('[data-native-record]')?.textContent === '原生模块第一份', 'NATIVE_MODULE_SLOT_BASELINE');
    await surfaceControl('pending');
    await rendered(() => container.querySelector('[data-native-eligibility]')?.textContent === 'pending', 'NATIVE_ROOT_ACTION_ELIGIBILITY_PENDING');
    await surfaceControl('denied');
    await rendered(() => container.querySelector('[data-native-eligibility]')?.textContent === '原生资料尚未确认', 'NATIVE_ROOT_ACTION_ELIGIBILITY_DENIED');
    await surfaceControl('allowed');
    await rendered(() => container.querySelector('[data-native-eligibility]')?.textContent === 'allowed', 'NATIVE_ROOT_ACTION_ELIGIBILITY_RECOVERED');
    await surfaceControl('update');
    await rendered(() => container.querySelector('[data-native-record]')?.textContent === '原生模块实际更新', 'NATIVE_MODULE_SLOT_FOLLOWS_REAL_FILE');
    check((await fetch('/native-file/update', { method: 'POST' })).ok, 'REAL_NATIVE_FILE_UPDATE');
    await rendered(() => container.querySelector('[data-native-file]')?.textContent === '原生文件实际更新', 'NATIVE_FILE_SLOT_FOLLOWS_ACTUAL_FS');
    const action = await second.session.module(layout.mounts.root).resolveAction('check', null);
    const invocation = await action({ amount: '7' });
    const rootLookup = await second.session.lookupInvocation('root.check', invocation.id);
    check(rootLookup.id === invocation.id, 'NATIVE_ROOT_LOOKUP_PRESERVES_ORIGINAL_INVOCATION');
    const nativeActivity = second.session.invocation(invocation);
    await invocation.result;
    check((await rootLookup.result).operation === 'operation-7', 'NATIVE_ROOT_LOOKUP_READS_ORIGINAL_RESULT');
    check((await firstValueFrom(invocation.activity$.pipe(filter(value => value.state === 'returned'), timeout(8000)))).state === 'returned', 'INVOCATION_ACTIVITY_TERMINAL_SNAPSHOT');
    check(nativeActivity.getSnapshot() === invocation.activity.peek(), 'NATIVE_INVOCATION_USES_THE_SAME_RESOURCE_FACT');
    check((await second.session.module(layout.mounts.root).readView('current')).toISOString() === '2026-10-26T00:00:07.000Z', 'PEER_REMAINS_LIVE');
    await rendered(() => container.querySelector('time')?.textContent === '2026-10-26T00:00:07.000Z', 'NATIVE_ROOT_RESOURCE_UPDATED');
    check(notifications === frozen, 'RELEASED_UI_OBSERVER_STOPPED');
    stop();

    await nativeGate('hold');
    actionHeld = true;
    const accepted = await action({ amount: '7' });
    await nativeGate('entered');
    await releaseView(); releaseView = undefined;
    check(container.childElementCount === 0 && mounted === 1 && unmounted === 1, 'NATIVE_RENDERER_UNMOUNTS_REAL_SLOT_TREE');
    await nativeGate('release');
    actionHeld = false;
    check((await accepted.result).at.toISOString() === '2026-10-26T00:00:07.000Z', 'UI_UNMOUNT_PRESERVES_ACCEPTED_BUSINESS');

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
    check(await reopened.session.module(layout.mounts.root).readView('current') instanceof Date, 'REOPENED_HOST_FACT_REMAINS');
    releaseView = await mountView(reopened.session);
    await rendered(() => container.querySelector('time')?.textContent === '2026-10-26T00:00:07.000Z', 'REOPENED_NATIVE_ROOT_RESOURCE');
    const finalGate = delayNextOpen();
    const ended = rejects(retainNativeSession(program, ctx, sessionId), error => String(error).includes('REFERENCE_RELEASED'));
    await finalGate.received.promise; await ctx.fiber.dispose(); await ended;
    finalGate.release.resolve(); delayed = undefined;
    check(reopened.session.scope.disposed, 'CONTEXT_RELEASES_OWNED_UI');
    check(container.childElementCount === 0 && mounted === 2 && unmounted === 2, 'CONTEXT_UNMOUNTS_NATIVE_RENDERER');
    const proof = await fetch('/proof').then(response => response.json());
    check(proof.executions === 2 && proof.models === 0, 'NO_EXTRA_BUSINESS_OR_MODEL_EXECUTION');
    check(proof.records.length === 2 && proof.records[0].command === rowKey + ':1'
      && proof.records[0].input === 'accepted-old-generation' && proof.records[1].command === 'overview:1'
      && proof.records[1].input === 'external-sdk-survives', 'REAL_FILES_PROVE_ONLY_EXPLICIT_MODULE_EFFECTS');
    check(sendsAfterClose === 0, 'NO_SEND_AFTER_PHYSICAL_SOCKET_CLOSE:' + sendsAfterClose);
    result = { passed: true, sessionId, models: proof.models, executions: proof.executions,
      sameGeneration: true, independentRelease: true, cancelledAcquisition: true, failedAcquisition: true, reopenedGeneration: true, contextClosedPending: true, sendsAfterClose, invocationActivity: true,
      nativeRenderer: true, react: React.version, domUpdated: true, unmountPreservesBusiness: true, contextUnmounted: true,
      nativeFiles: true, uiFileReaderSharesFact: true, uiWatchClosed: true, borrowedSdkRemainsActive: true,
      nativeModules: true, dynamicRemount: true, sameSourceEligibility: true, moduleInvocationLookup: true,
      operationCommandPreserved: true, moduleUiExit: true };
  } catch (error) { failures.push(error); }
  finally {
    await cleanup(() => delayed?.release.resolve());
    if (actionHeld) await cleanup(() => nativeGate('release'));
    await cleanup(() => releaseView?.());
    await cleanup(() => borrowedUi?.dispose()); await cleanup(() => borrowed?.release());
    const released = await Promise.allSettled(leases.map(lease => Promise.resolve().then(() => lease.release())));
    failures.push(...released.filter(result => result.status === 'rejected').map(result => result.reason));
    await cleanup(() => ctx.fiber.dispose());
    await cleanup(() => transport?.dispose());
  }
  if (failures.length) {
    const error = failures.length === 1 ? failures[0] : new AggregateError(failures, 'BROWSER_NATIVE_CONSUMER_FAILED');
    const describe = error => ({ error: String(error), stack: error?.stack,
      ...(error instanceof AggregateError ? { errors: error.errors.map(describe) } : {}) });
    console.error(error);
    result = { passed: false, ...describe(error), errors: failures.map(describe) };
  }
  output.textContent = JSON.stringify(result);
});
output.textContent = '已装配正式 rc.3 Client Sessions 与 Slot renderer，可验证资源呈现及引用归属';
document.querySelector('button').disabled = false;
