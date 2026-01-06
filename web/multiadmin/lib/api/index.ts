// Re-export client and error class
export { MultiAdminClient, ApiError } from "./client";
export type { ApiClientConfig } from "./client";

// Re-export React context
export { ApiProvider, useApi } from "./context";

// Re-export all generated types from public API
export * from "./generated/multiadminservice.pb";
export * from "./generated/clustermetadata.pb";
// Note: multipoolermanagerdata.pb is not exported as it's used internally
