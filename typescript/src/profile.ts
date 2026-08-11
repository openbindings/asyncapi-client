import type { AsyncAPIExecutionProfile } from "./internal/index.js";

export type { AsyncAPIExecutionProfile } from "./internal/index.js";

/** Current artifact-execution behavior for the supported AsyncAPI editions. */
export const ASYNCAPI_PROFILE_FULL: AsyncAPIExecutionProfile = Object.freeze({
  name: "asyncapi-2.0-3.1",
});
