import type { AsyncAPIExecutionProfile } from "./internal/index.js";

export type { AsyncAPIExecutionProfile } from "./internal/index.js";

/** Historical execution behavior retained only for immutable adapter compatibility. */
export const ASYNCAPI_PROFILE_COMPAT: AsyncAPIExecutionProfile = Object.freeze({
  name: "asyncapi-3.0-compat",
  preserveSendReplies: false,
});

/** Full currently qualified artifact-execution behavior. */
export const ASYNCAPI_PROFILE_FULL: AsyncAPIExecutionProfile = Object.freeze({
  name: "asyncapi-3.0-full",
  preserveSendReplies: true,
});
