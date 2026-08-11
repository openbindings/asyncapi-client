import type { InvokeHooks, InvokeSite } from "./hooks.js";

/** Internal execution profile used to preserve immutable adapter behavior. */
export interface AsyncAPIExecutionProfile {
  readonly name: string;
  readonly preserveSendReplies: boolean;
}

export interface InvocationSource {
  profile: AsyncAPIExecutionProfile;
  location?: string;
  content?: unknown;
}

export interface BindingInvocationArgs {
  source: InvocationSource;
  ref: string;
  context?: Record<string, unknown>;
  maxDeliveryUnitBytes?: number;
  signal?: AbortSignal;
  fetch?: typeof globalThis.fetch;
  observeOutput?: (value: unknown, metadata: Record<string, string[]>) => void;
  hooks?: InvokeHooks | null;
  site?: InvokeSite;
  /** Adapter hint for an operation surface that deliberately accepts no input. */
  acceptsInput?: boolean;
}

export const DEFAULT_MAX_DELIVERY_UNIT_BYTES = 10 * 1024 * 1024;

export function resolveDeliveryUnitLimit(
  args: Pick<BindingInvocationArgs, "maxDeliveryUnitBytes">,
): number {
  const value = args.maxDeliveryUnitBytes;
  return value !== undefined && Number.isFinite(value) && value > 0
    ? value
    : DEFAULT_MAX_DELIVERY_UNIT_BYTES;
}
