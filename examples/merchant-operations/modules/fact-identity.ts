import type { MerchantTarget, OnboardingTarget } from '../contracts/index.ts';

export function assertOnboarding(actual: OnboardingTarget, expected: OnboardingTarget): void {
  if (actual.onboardingId !== expected.onboardingId) throw new Error('MERCHANT_MODULE_ONBOARDING_MISMATCH');
}

export function assertMerchant(actual: MerchantTarget, expected: MerchantTarget): void {
  if (actual.subject.kind !== expected.subject.kind || actual.subject.id !== expected.subject.id
    || actual.sandboxId !== expected.sandboxId || actual.deliveryIdentity !== expected.deliveryIdentity)
    throw new Error('MERCHANT_MODULE_RUNTIME_TARGET_MISMATCH');
}
