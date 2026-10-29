import { test, expect } from '@playwright/test';
import { writeFile } from 'node:fs/promises';

test('实际安装包的浏览器消费与退出', async ({ page }, testInfo) => {
  const { route, button, output } = testInfo.project.metadata;
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.goto(route);
  await page.locator(button).click();
  const receipt = page.locator(output);
  await expect(receipt).toHaveText(/^\{"passed":(?:true|false),/u, { timeout: 45_000 });
  const proof = JSON.parse(await receipt.textContent());
  // 页面复用既有完整行为断言，并在实际异步清理结算后才提供此回执。
  expect(proof, JSON.stringify(proof)).toMatchObject({ passed: true });
  expect(errors).toEqual([]);
  const path = testInfo.outputPath('consumer-proof.json');
  await writeFile(path, JSON.stringify(proof, null, 2) + '\n');
  await testInfo.attach('consumer-proof', { path, contentType: 'application/json' });
});
