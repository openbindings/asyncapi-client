import type {
  AsyncAPIDocument,
  AsyncAPIMessage,
  AsyncAPIOperation,
} from "./asyncapi-types.js";
import {
  messageEffectiveContentType,
  supportedMessageContentType,
} from "./content.js";

/** True when the current execution profile can carry one declared message. */
export function messageBindable(
  doc: AsyncAPIDocument,
  message: AsyncAPIMessage,
): boolean {
  if (message["x-ob-asyncapi-unresolved-trait"] !== undefined) return false;
  if (message.headers !== undefined) return false;
  const version = message.bindings?.http?.bindingVersion;
  if (version !== undefined && version !== "0.3.0") return false;
  try {
    supportedMessageContentType(messageEffectiveContentType(doc, message));
    return true;
  } catch {
    return false;
  }
}

/** True when every reply declaration has faithful current-profile carriage. */
export function replyMessagesBindable(
  doc: AsyncAPIDocument,
  operation: AsyncAPIOperation,
): boolean {
  if (!operation.reply) return true;
  const messages = operation.reply.messages?.length
    ? operation.reply.messages
    : Object.values(operation.reply.channel?.messages ?? {});
  return messages.length > 0 && messages.every((message) => messageBindable(doc, message));
}
