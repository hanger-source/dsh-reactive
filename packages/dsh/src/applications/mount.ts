import { realpath } from "node:fs/promises";
import { isAbsolute } from "node:path";
import type { Context, Fiber } from "@deepseek-ai/cordis";
import { programDependencies, type ApplicationDefinition } from "@hanger-source/dsh-reactive/kernel";
import { type AnyEnvironment } from "@hanger-source/dsh-reactive/kernel";
import { Workspaces } from "../workspaces/workspaces.ts";
import { collectOperationOwners, createApplicationAssembly, type ApplicationAssembly } from './assembly.ts';
import { moduleNativeDependencies } from '../programs/modules.ts';
import { ApplicationHost } from '../application.ts';
import { requireActivePlugin } from '../native-installation.ts';
import type { HostOptions } from './options.ts';
export type ApplicationFactory<E extends readonly AnyEnvironment[] = readonly AnyEnvironment[]> = (assembly: ApplicationAssembly) => ApplicationDefinition<E>;
/** @internal Runtime与嵌入宿主共用一次工厂求值和失败释放。 */
export async function assembleApplication<E extends readonly AnyEnvironment[]>(ctx: Context, factory: ApplicationFactory<E>) {
    const state = createApplicationAssembly(ctx);
    let closing: Promise<void> | undefined;
    const release = () => {
        if (closing) return closing;
        const completion = Promise.withResolvers<void>();
        closing = completion.promise;
        void (async () => {
            const failures = new Set<unknown>();
            for (const capability of [...state.complete().capabilities].reverse()) {
                try { await capability.dispose(); } catch (error) { failures.add(error); }
            }
            state.native.dispose();
            if (failures.size === 1) throw [...failures][0];
            if (failures.size) throw new AggregateError([...failures], 'REACTIVE_APPLICATION_CAPABILITIES_DISPOSAL_FAILED');
        })().then(completion.resolve, completion.reject);
        return closing;
    };
    try {
        const definition = factory(state.assembly);
        const programs = [...definition.environments.map(environment => environment.program), ...state.native.programs()];
        for (const program of programs) {
            const dependencies = programDependencies(program);
            for (const module of dependencies.modules) {
                const capabilities = moduleNativeDependencies(module);
                if (capabilities) for (const capability of capabilities) state.assembly.native(capability);
            }
            for (const owner of dependencies.operations) state.assembly.operation(owner);
        }
        return { definition, native: state.native, ...state.complete(), release };
    } catch (error) {
        try { await release(); }
        catch (cleanup) { throw new AggregateError([error, cleanup], 'REACTIVE_APPLICATION_ASSEMBLY_FAILED'); }
        throw error;
    }
}
export async function mountApplication<E extends readonly AnyEnvironment[]>(ctx: Context, factory: ApplicationFactory<E>, options: HostOptions = {}): Promise<ApplicationHost<E>> {
    return mountAssembledApplication(ctx, await assembleApplication(ctx, factory), options);
}
/** @internal 只挂载已经封闭的同次装配，不向消费者暴露安装实例。 */
export async function mountAssembledApplication<E extends readonly AnyEnvironment[]>(ctx: Context, assembled: Awaited<ReturnType<typeof assembleApplication<E>>>, options: HostOptions = {}): Promise<ApplicationHost<E>> {
    const { definition, native: delegations, capabilities } = assembled;
    let host: ApplicationHost<E> | undefined;
    let installation: (Fiber & PromiseLike<Fiber>) | undefined;
    const releaseInstallation = async () => {
        const failures = new Set<unknown>();
        if (installation) {
            try { await installation.dispose(); } catch (error) { failures.add(error); }
            try { await installation; } catch (error) { failures.add(error); }
        }
        try { await assembled.release(); } catch (error) { failures.add(error); }
        if (failures.size === 1) throw [...failures][0];
        if (failures.size) throw new AggregateError([...failures], 'REACTIVE_APPLICATION_INSTALLATION_FAILED');
    };
    try {
        let home: string | undefined;
        if (capabilities.length) {
            if (!options.home) throw new Error('REACTIVE_NATIVE_HOME_REQUIRED');
            if (!isAbsolute(options.home)) throw new Error('REACTIVE_HOME_NOT_ABSOLUTE');
            home = await realpath(options.home);
        }
        const environments: AnyEnvironment[] = [];
        for (const environment of definition.environments) {
            const skills: string[] = [];
            for (const directory of environment.skills ?? []) {
                if (!isAbsolute(directory)) throw new Error('REACTIVE_SKILL_PATH_NOT_ABSOLUTE');
                skills.push(await realpath(directory));
            }
            environments.push({ ...environment, skills });
        }
        const hostOptions = { ...options, operations: collectOperationOwners([...options.operations ?? [], ...assembled.operations]) };
        const required = ['agents', 'sessions', 'sessionPersistence', 'sessionQuery', 'sessionProjections', 'workspaceRegistry', 'storageDomain',
            'goals', 'tools', 'systemPrompt', 'loader', 'agentPresets', 'skills', 'fs', 'workingDirectory', 'userQuestions', 'attachments', 'fileUploads'];
        if (delegations.programs().length) required.push('subagents');
        installation = ctx.plugin({ name: 'reactive-application:' + definition.name, async apply(application: Context) {
            let hostInstallation: (Fiber & PromiseLike<Fiber>) | undefined;
            // 同步generator保留thenable disposer的原身份；async yield会先将它解析成另一函数。
            application.effect(function* () {
                yield () => assembled.release();
                const nativeInstallation = application.plugin({ name: 'reactive-application-capabilities', async apply(native: Context) {
                    for (const capability of capabilities) await capability.install(native, home!);
                } });
                yield nativeInstallation.dispose;
                hostInstallation = application.plugin({ name: 'reactive-application-host', async apply(scope: Context) {
                    await requireActivePlugin(nativeInstallation);
                    await requireActivePlugin(scope.inject(required, async native => {
                        if (host) throw new Error('REACTIVE_APPLICATION_INSTALLATION_ENDED');
                        const workspaces = new Workspaces(native, definition.name, environments);
                        try {
                            host = ApplicationHost.create(native, definition, workspaces, hostOptions, delegations, releaseInstallation);
                            await host.initialize();
                            native.effect(() => () => {
                                // 必需服务退出同时撤销本次应用安装；不等待正在卸载自己的子Fiber。
                                if (installation?.state === 1 || installation?.state === 2) {
                                    void installation.dispose().catch(error => console.error('reactive.application.disposal.failed', error));
                                }
                            });
                        } catch (error) {
                            try { if (host) await host.drain(); else workspaces.dispose(); }
                            catch (cleanup) { throw new AggregateError([error, cleanup], 'REACTIVE_APPLICATION_STARTUP_FAILED'); }
                            throw error;
                        }
                    }));
                    if (!host) throw new Error('REACTIVE_DSH_SERVICES_REQUIRED');
                } });
                yield hostInstallation.dispose;
                yield () => host?.drain().catch(error => console.error('reactive.application.drain.failed', error));
            });
            await requireActivePlugin(hostInstallation!);
        } });
        await requireActivePlugin(installation);
        return host!;
    } catch (error) {
        try { await releaseInstallation(); }
        catch (cleanup) {
            if (cleanup !== error) throw new AggregateError([error, cleanup], 'REACTIVE_APPLICATION_STARTUP_FAILED');
        }
        throw error;
    }
}
