import { realpath } from "node:fs/promises";
import { isAbsolute } from "node:path";
import type { Context } from "@deepseek-ai/cordis";
import { type ApplicationDefinition } from "@hanger-source/dsh-reactive/kernel";
import { type AnyEnvironment } from "@hanger-source/dsh-reactive/kernel";
import { Workspaces } from "../workspaces/workspaces.ts";
import { createApplicationAssembly, type ApplicationAssembly } from "../delegations.ts";
import { ApplicationHost } from '../application.ts';
import type { HostOptions } from './options.ts';
export type ApplicationFactory<E extends readonly AnyEnvironment[] = readonly AnyEnvironment[]> = (assembly: ApplicationAssembly) => ApplicationDefinition<E>;
export function assembleApplication<E extends readonly AnyEnvironment[]>(ctx: Context, factory: ApplicationFactory<E>) {
    const { assembly, native } = createApplicationAssembly(ctx);
    return { definition: factory(assembly), native };
}
export async function mountApplication<E extends readonly AnyEnvironment[]>(ctx: Context, factory: ApplicationFactory<E>, options: HostOptions = {}): Promise<ApplicationHost<E>> {
    return mountAssembledApplication(ctx, assembleApplication(ctx, factory), options);
}
export async function mountAssembledApplication<E extends readonly AnyEnvironment[]>(ctx: Context, assembled: ReturnType<typeof assembleApplication<E>>, options: HostOptions = {}): Promise<ApplicationHost<E>> {
    const { definition, native: delegations } = assembled;
    const environments: AnyEnvironment[] = [];
    for (const environment of definition.environments) {
        const skills: string[] = [];
        for (const directory of environment.skills ?? []) {
            if (!isAbsolute(directory))
                throw new Error('REACTIVE_SKILL_PATH_NOT_ABSOLUTE');
            skills.push(await realpath(directory));
        }
        environments.push({ ...environment, skills });
    }
    let host: ApplicationHost<E> | undefined;
    const required = ['agents', 'sessions', 'sessionPersistence', 'sessionQuery', 'sessionProjections', 'workspaceRegistry',
        'goals', 'tools', 'systemPrompt', 'loader', 'agentPresets', 'skills', 'fs', 'workingDirectory', 'userQuestions', 'attachments', 'fileUploads'];
    if (delegations.programs().length)
        required.push('subagents');
    const owner = ctx.inject(required, async (native) => {
        const workspaces = new Workspaces(native, definition.name, environments);
        try {
            host = ApplicationHost.create(native, definition, workspaces, options, delegations);
            await host.initialize();
        }
        catch (error) {
            const failures: unknown[] = [error];
            try {
                if (host)
                    await host.dispose();
                else
                    workspaces.dispose();
            }
            catch (cleanup) {
                failures.push(cleanup);
            }
            if (failures.length > 1)
                throw new AggregateError(failures, 'REACTIVE_APPLICATION_STARTUP_FAILED');
            throw error;
        }
    });
    try {
        await owner;
    }
    catch (error) {
        try {
            await owner.dispose();
        }
        catch (cleanup) {
            throw new AggregateError([error, cleanup], 'REACTIVE_APPLICATION_STARTUP_FAILED');
        }
        throw error;
    }
    if (!host) {
        await owner.dispose();
        throw new Error('REACTIVE_DSH_SERVICES_REQUIRED');
    }
    return host;
}
