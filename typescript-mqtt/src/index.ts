import {
  connect,
  type IClientOptions,
  type IClientPublishOptions,
  type IClientSubscribeOptions,
  type MqttClient,
} from "mqtt";
import { createHash } from "node:crypto";
import type {
  AsyncAPIProtocolDriver,
  AsyncAPIProtocolDriverRequest,
  AsyncAPIProtocolDriverSession,
} from "@openbindings/asyncapi-client";

export interface AsyncAPIMQTTDriverOptions {
  /** Explicit completion for an artifact that omits server.protocolVersion. */
  protocolVersion?: "3.1.1";
  /** Explicit completion for an artifact that omits its client identifier. */
  clientId?: string;
  /** TLS material for mqtts targets. */
  tls?: Pick<IClientOptions, "ca" | "cert" | "key" | "rejectUnauthorized" | "servername">;
  /** Exact protocol spellings this instance owns. Defaults to qualified `mqtt`. */
  protocols?: readonly string[];
  /** Test/embedding seam; ordinary consumers use MQTT.js. */
  connector?: (url: string, options: IClientOptions) => MqttClient;
}

/** MQTT 3.1.1 execution under AsyncAPI MQTT binding versions 0.1.0 and 0.2.0. */
export class AsyncAPIMQTTDriver implements AsyncAPIProtocolDriver {
  readonly protocols: readonly string[];
  private readonly connections = new Map<string, MQTTConnection>();

  constructor(private readonly options: AsyncAPIMQTTDriverOptions = {}) {
    this.protocols = options.protocols ?? ["mqtt"];
  }

  async execute(
    request: AsyncAPIProtocolDriverRequest,
    session: AsyncAPIProtocolDriverSession,
  ): Promise<void> {
    const profile = resolveProfile(request, this.options);
    const clientOptions = mqttClientOptions(request, profile, this.options);
    const connection = await this.acquire(request, profile, clientOptions);

    try {
      if (request.action === "receive") {
        await publishInputs(connection.client, request, session, profile);
      } else {
        await subscribeOutputs(connection, request, session, profile);
      }
    } finally {
      await this.release(connection);
    }
  }

  private async acquire(
    request: AsyncAPIProtocolDriverRequest,
    profile: MQTTProfile,
    clientOptions: IClientOptions,
  ): Promise<MQTTConnection> {
    const key = connectionKey(request, profile, clientOptions);
    const existing = this.connections.get(key);
    if (existing) {
      existing.users++;
      try {
        await existing.ready;
        return existing;
      } catch (error: unknown) {
        existing.users--;
        throw error;
      }
    }

    const client = (this.options.connector ?? connect)(request.serverURL, clientOptions);
    const connection: MQTTConnection = {
      key,
      client,
      users: 1,
      subscribers: new Map(),
      ready: Promise.resolve(),
    };
    client.handleMessage = (packet, done) => {
      const subscribers = [...(connection.subscribers.get(packet.topic) ?? [])];
      void subscribers.reduce(
        (chain, subscriber) => chain.then(() => subscriber.deliver(packet.payload)),
        Promise.resolve(),
      ).then(() => done(), () => done());
    };
    connection.ready = waitForConnect(client, request.signal).catch(async (error: unknown) => {
      if (this.connections.get(key) === connection) this.connections.delete(key);
      await endClient(client);
      throw error;
    });
    this.connections.set(key, connection);
    await connection.ready;
    return connection;
  }

  private async release(connection: MQTTConnection): Promise<void> {
    connection.users--;
    if (connection.users > 0) return;
    if (this.connections.get(connection.key) === connection) this.connections.delete(connection.key);
    await endClient(connection.client);
  }
}

export function createAsyncAPIMQTTDriver(
  options: AsyncAPIMQTTDriverOptions = {},
): AsyncAPIMQTTDriver {
  return new AsyncAPIMQTTDriver(options);
}

interface MQTTProfile {
  qos: 0 | 1 | 2;
  retain: boolean;
  clean: boolean;
  keepalive: number;
  clientId?: string;
}

interface MQTTSubscriber {
  deliver(payload: Buffer | string): Promise<void>;
  fail(error: unknown): void;
}

interface MQTTConnection {
  key: string;
  client: MqttClient;
  users: number;
  ready: Promise<void>;
  subscribers: Map<string, Set<MQTTSubscriber>>;
}

function resolveProfile(
  request: AsyncAPIProtocolDriverRequest,
  options: AsyncAPIMQTTDriverOptions,
): MQTTProfile {
  if (
    request.operation["x-ob-asyncapi-v2-security-conjunction"] !== undefined
    || request.server?.["x-ob-asyncapi-v2-security-conjunction"] !== undefined
  ) {
    throw new Error("the MQTT 3.1.1 profile does not admit normalized AsyncAPI 2.x multi-scheme security conjunctions");
  }
  if (request.operation["reply"] !== undefined) {
    throw new Error("the MQTT 3.1.1 driver does not admit AsyncAPI reply operations");
  }
  const protocolVersion = stringValue(request.server?.["protocolVersion"])
    || mqttConfiguration(request)["protocolVersion"]
    || options.protocolVersion;
  if (protocolVersion !== "3.1.1") {
    throw new Error("MQTT execution requires server.protocolVersion or configuration.mqtt.protocolVersion to select exactly 3.1.1");
  }

  const serverBinding = binding(request.server, "mqtt");
  const operationBinding = binding(request.operation, "mqtt");
  const channelBinding = binding(request.channel, "mqtt");
  validateBindingVersion(serverBinding, "server");
  validateBindingVersion(operationBinding, "operation");
  validateAllowedFields(serverBinding, [
    "clientId", "cleanSession", "lastWill", "keepAlive", "sessionExpiryInterval",
    "maximumPacketSize", "bindingVersion",
  ], "server");
  validateAllowedFields(operationBinding, ["qos", "retain", "messageExpiryInterval", "bindingVersion"], "operation");
  if (Object.keys(channelBinding).length > 0) {
    throw new Error("the AsyncAPI MQTT 0.2.0 channel binding must be empty");
  }
  validateTopic(request.address, request.action);
  for (const message of request.messages) {
    const messageBinding = binding(message, "mqtt");
    validateBindingVersion(messageBinding, "message");
    const mqtt5Fields = Object.keys(messageBinding).filter((key) => key !== "bindingVersion");
    if (mqtt5Fields.length > 0) {
      throw new Error(`MQTT 3.1.1 cannot apply MQTT 5 message-binding fields: ${mqtt5Fields.sort().join(", ")}`);
    }
  }

  const qos = qosValue(operationBinding["qos"] ?? 0, "operation qos");
  const retain = booleanValue(operationBinding["retain"] ?? false, "operation retain");
  if (request.action === "send" && operationBinding["retain"] !== undefined) {
    throw new Error("the MQTT retain operation-binding field applies only when publishing");
  }
  if (operationBinding["messageExpiryInterval"] !== undefined) {
    throw new Error("messageExpiryInterval is MQTT 5-only and is outside the MQTT 3.1.1 driver profile");
  }

  const clean = booleanValue(serverBinding["cleanSession"] ?? true, "server cleanSession");
  if (!clean) {
    throw new Error("persistent MQTT sessions are outside the currently qualified MQTT 3.1.1 profile");
  }
  const keepalive = integerValue(serverBinding["keepAlive"] ?? 60, "server keepAlive", 0, 65535);
  if (serverBinding["sessionExpiryInterval"] !== undefined || serverBinding["maximumPacketSize"] !== undefined) {
    throw new Error("the selected MQTT server binding uses MQTT 5-only fields");
  }
  if (serverBinding["lastWill"] !== undefined) {
    throw new Error("MQTT Last Will is outside the currently qualified MQTT 3.1.1 profile");
  }
  const configuredClientId = mqttConfiguration(request)["clientId"];
  const clientId = stringValue(serverBinding["clientId"])
    || (typeof configuredClientId === "string" ? configuredClientId : undefined)
    || options.clientId;
  if (!clean && !clientId) {
    throw new Error("a persistent MQTT session requires an artifact- or configuration-selected clientId");
  }

  return {
    qos,
    retain,
    clean,
    keepalive,
    ...(clientId ? { clientId } : {}),
  };
}

function mqttClientOptions(
  request: AsyncAPIProtocolDriverRequest,
  profile: MQTTProfile,
  options: AsyncAPIMQTTDriverOptions,
): IClientOptions {
  const target = new URL(request.serverURL);
  if (target.protocol !== "mqtt:" && target.protocol !== "mqtts:") {
    throw new Error(`MQTT driver cannot execute target scheme ${JSON.stringify(target.protocol)}`);
  }
  if (target.pathname !== "/" && target.pathname !== "") {
    throw new Error("MQTT TCP targets cannot apply an AsyncAPI server pathname");
  }
  const security = selectSecurity(request, options, target.protocol);
  return {
    protocolVersion: 4,
    reconnectPeriod: 0,
    resubscribe: false,
    clean: profile.clean,
    keepalive: profile.keepalive,
    ...(profile.clientId ? { clientId: profile.clientId } : {}),
    ...(security.basic ? { username: security.basic.username, password: security.basic.password } : {}),
    ...(target.protocol === "mqtts:" ? options.tls : {}),
  };
}

async function publishInputs(
  client: MqttClient,
  request: AsyncAPIProtocolDriverRequest,
  session: AsyncAPIProtocolDriverSession,
  profile: MQTTProfile,
): Promise<void> {
  if (!request.encodeInput) throw new Error("MQTT publish request has no artifact codec");
  let count = 0;
  for await (const value of session.inputs) {
    if (request.signal.aborted) return;
    const payload = request.encodeInput(value);
    await publish(client, request.address, payload, { qos: profile.qos, retain: profile.retain });
    count++;
  }
  if (count === 0) throw new Error("MQTT publish invocation requires at least one input value");
}

async function subscribeOutputs(
  connection: MQTTConnection,
  request: AsyncAPIProtocolDriverRequest,
  session: AsyncAPIProtocolDriverSession,
  profile: MQTTProfile,
): Promise<void> {
  if (!request.decodeOutput) throw new Error("MQTT subscription request has no artifact codec");
  const decodeOutput = request.decodeOutput;
  await session.closeInput();
  let failSubscriber: (error: unknown) => void = () => undefined;
  const failed = new Promise<never>((_resolve, reject) => { failSubscriber = reject; });
  const subscriber: MQTTSubscriber = {
    deliver: async (payloadValue) => {
      try {
        const payload = typeof payloadValue === "string" ? Buffer.from(payloadValue) : payloadValue;
        await session.emit(await decodeOutput(new Uint8Array(payload)));
      } catch (error: unknown) {
        failSubscriber(error);
        throw error;
      }
    },
    fail: failSubscriber,
  };
  let subscribers = connection.subscribers.get(request.address);
  if (!subscribers) {
    subscribers = new Set();
    connection.subscribers.set(request.address, subscribers);
  }
  subscribers.add(subscriber);
  try {
    await subscribe(connection.client, request.address, { qos: profile.qos });
    session.setLeadingMetadata({ "mqtt-subscription": ["ready"] });
    await Promise.race([untilStopped(connection.client, request.signal), failed]);
  } finally {
    subscribers.delete(subscriber);
    if (subscribers.size === 0) connection.subscribers.delete(request.address);
  }
}

function connectionKey(
  request: AsyncAPIProtocolDriverRequest,
  profile: MQTTProfile,
  options: IClientOptions,
): string {
  const identity = JSON.stringify({
    url: request.serverURL,
    protocolVersion: options.protocolVersion,
    clean: profile.clean,
    keepalive: profile.keepalive,
    clientId: profile.clientId ?? "",
    username: options.username ?? "",
    password: options.password ?? "",
    tlsServername: options.servername ?? "",
  });
  return createHash("sha256").update(identity).digest("hex");
}

function binding(
  owner: Readonly<Record<string, unknown>> | undefined,
  name: string,
): Record<string, unknown> {
  const bindings = record(owner?.["bindings"]);
  return record(bindings?.[name]) ?? {};
}

function validateBindingVersion(bindingValue: Record<string, unknown>, location: string): void {
  const version = bindingValue["bindingVersion"] ?? "0.2.0";
  if (version !== "0.1.0" && version !== "0.2.0") {
    throw new Error(`MQTT ${location} binding version ${JSON.stringify(version)} is outside the 0.1.0/0.2.0 driver profile`);
  }
}

function validateAllowedFields(bindingValue: Record<string, unknown>, allowed: readonly string[], location: string): void {
  const unknown = Object.keys(bindingValue).filter((name) => !allowed.includes(name)).sort();
  if (unknown.length > 0) {
    throw new Error(`MQTT ${location} binding contains undeclared fields: ${unknown.join(", ")}`);
  }
}

function validateTopic(topic: string, action: "send" | "receive"): void {
  if (topic.length === 0 || Buffer.byteLength(topic, "utf8") > 65_535 || topic.includes("\u0000") || hasUnpairedSurrogate(topic)) {
    throw new Error("MQTT topic must be a non-empty, valid UTF-8 string no larger than 65535 bytes");
  }
  if (action === "receive") {
    if (topic.includes("#") || topic.includes("+")) {
      throw new Error("MQTT publish topic names cannot contain wildcard characters");
    }
    return;
  }
  const levels = topic.split("/");
  for (const [index, level] of levels.entries()) {
    if ((level.includes("#") && (level !== "#" || index !== levels.length - 1)) || (level.includes("+") && level !== "+")) {
      throw new Error("MQTT subscription wildcards must occupy an entire level and # must be final");
    }
  }
}

function hasUnpairedSurrogate(value: string): boolean {
  for (let index = 0; index < value.length; index++) {
    const code = value.charCodeAt(index);
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(++index);
      if (!(next >= 0xdc00 && next <= 0xdfff)) return true;
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      return true;
    }
  }
  return false;
}

function mqttConfiguration(request: AsyncAPIProtocolDriverRequest): Record<string, unknown> {
  const configuration = record(request.context?.["configuration"]);
  return record(configuration?.["mqtt"]) ?? {};
}

function selectSecurity(
  request: AsyncAPIProtocolDriverRequest,
  options: AsyncAPIMQTTDriverOptions,
  protocol: string,
): { basic?: { username: string; password: string }; x509: boolean } {
  if (request.securityAlternatives.length === 0) return { x509: false };
  const basic = basicCredential(request.context);
  const hasX509 = protocol === "mqtts:" && Boolean(options.tls?.cert && options.tls?.key);
  for (const alternative of request.securityAlternatives) {
    let usesBasic = false;
    let usesX509 = false;
    let satisfiable = alternative.length > 0;
    for (const { scheme } of alternative) {
      switch (scheme["type"]) {
        case "userPassword":
          usesBasic = true;
          satisfiable &&= basic !== undefined;
          break;
        case "X509":
          usesX509 = true;
          satisfiable &&= hasX509;
          break;
        default:
          satisfiable = false;
      }
    }
    if (satisfiable) return { ...(usesBasic && basic ? { basic } : {}), x509: usesX509 };
  }
  const types = [...new Set(request.securityAlternatives.flatMap((alternative) => alternative.map(({ scheme }) => String(scheme["type"]))))];
  throw new Error(`no declared AsyncAPI security alternative can be satisfied by the MQTT 3.1.1 driver (${types.join(", ")})`);
}

function basicCredential(context: Readonly<Record<string, unknown>> | undefined): { username: string; password: string } | undefined {
  const basic = record(context?.["basic"]);
  const username = stringValue(basic?.["username"]);
  const password = stringValue(basic?.["password"]);
  return username || password ? { username: username ?? "", password: password ?? "" } : undefined;
}

function qosValue(value: unknown, name: string): 0 | 1 | 2 {
  if (value === 0 || value === 1 || value === 2) return value;
  throw new Error(`${name} must be 0, 1, or 2`);
}

function booleanValue(value: unknown, name: string): boolean {
  if (typeof value === "boolean") return value;
  throw new Error(`${name} must be boolean`);
}

function integerValue(value: unknown, name: string, minimum: number, maximum: number): number {
  if (typeof value === "number" && Number.isInteger(value) && value >= minimum && value <= maximum) return value;
  throw new Error(`${name} must be an integer from ${minimum} through ${maximum}`);
}

function record(value: unknown): Record<string, unknown> | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined;
}

function stringValue(value: unknown): string | undefined {
  return typeof value === "string" && value !== "" ? value : undefined;
}

function waitForConnect(client: MqttClient, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const cleanup = () => {
      client.off("connect", connected);
      client.off("error", failed);
      signal.removeEventListener("abort", aborted);
    };
    const connected = () => { cleanup(); resolve(); };
    const failed = (error: Error) => { cleanup(); reject(error); };
    const aborted = () => { cleanup(); reject(signal.reason ?? new Error("MQTT connection cancelled")); };
    client.once("connect", connected);
    client.once("error", failed);
    signal.addEventListener("abort", aborted, { once: true });
    if (signal.aborted) aborted();
  });
}

function publish(client: MqttClient, topic: string, payload: Uint8Array, options: IClientPublishOptions): Promise<void> {
  return new Promise((resolve, reject) => {
    client.publish(topic, Buffer.from(payload), options, (error) => error ? reject(error) : resolve());
  });
}

function subscribe(client: MqttClient, topic: string, options: IClientSubscribeOptions): Promise<void> {
  return new Promise((resolve, reject) => {
    client.subscribe(topic, options, (error) => error ? reject(error) : resolve());
  });
}

function untilStopped(client: MqttClient, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const cleanup = () => {
      client.off("error", failed);
      client.off("close", closed);
      signal.removeEventListener("abort", aborted);
    };
    const aborted = () => { cleanup(); resolve(); };
    const failed = (error: Error) => { cleanup(); reject(error); };
    const closed = () => {
      cleanup();
      if (signal.aborted) resolve();
      else reject(new Error("MQTT connection closed before invocation cancellation"));
    };
    client.once("error", failed);
    client.once("close", closed);
    signal.addEventListener("abort", aborted, { once: true });
    if (signal.aborted) aborted();
  });
}

function endClient(client: MqttClient): Promise<void> {
  if (client.disconnected) return Promise.resolve();
  return new Promise((resolve) => client.end(false, {}, () => resolve()));
}
