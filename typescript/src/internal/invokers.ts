import type { ContextRequiredDetails } from "./invocation.js";

export type ContextResolver = (
  details: ContextRequiredDetails,
) => Promise<Record<string, unknown> | null> | Record<string, unknown> | null;
