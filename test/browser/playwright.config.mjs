import { defineConfig } from '@playwright/test';
import { isAbsolute, join } from 'node:path';

const required = name => {
  const value = process.env[name];
  if (!value || !isAbsolute(value)) throw new Error('PACKED_BROWSER_ABSOLUTE_DIRECTORY_REQUIRED: ' + name);
  return value;
};
const backend = required('DSH_REACTIVE_BROWSER_BACKEND');
const nativeAssets = required('DSH_REACTIVE_BROWSER_NATIVE_ASSETS');
const output = required('DSH_REACTIVE_BROWSER_OUTPUT');
const consumers = [
  { name: 'sdk', port: 41731, host: 'browser-host.mjs', route: '/', button: 'button', output: 'output' },
  { name: 'native', port: 41732, host: 'browser-host.mjs', route: '/native', button: 'button', output: 'output' },
  { name: 'requests', port: 41733, host: 'browser-request-host.mjs', route: '/', button: '#requests', output: '#status' },
];
// command 是 shell 接口：路径按 POSIX 单引号编码，空格与 shell 元字符均保持字面值。
const quote = value => "'" + value.replaceAll("'", "'\\''") + "'";
const origin = port => 'http://127.0.0.1:' + port;

export default defineConfig({
  testDir: '.', testMatch: 'packed-consumer.spec.mjs',
  workers: 1, retries: 0, timeout: 60_000, globalTimeout: 180_000,
  outputDir: join(output, 'results'),
  reporter: [['line'], ['json', { outputFile: join(output, 'report.json') }]],
  use: { browserName: 'chromium', headless: true, trace: 'retain-on-failure', screenshot: 'only-on-failure' },
  projects: consumers.map(({ name, port, route, button, output }) => ({
    name, use: { baseURL: origin(port) }, metadata: { route, button, output },
  })),
  webServer: consumers.map(({ name, port, host }) => ({
    name, cwd: backend,
    command: [process.execPath, join(backend, 'browser-host', host), '--port=' + port,
      ...(name === 'native' ? ['--native-assets=' + nativeAssets] : [])].map(quote).join(' '),
    url: origin(port), reuseExistingServer: false, timeout: 60_000,
    gracefulShutdown: { signal: 'SIGTERM', timeout: 10_000 }, stdout: 'pipe', stderr: 'pipe',
  })),
});
