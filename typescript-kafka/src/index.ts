import type {
  AsyncAPIProtocolDriver,
  AsyncAPIProtocolDriverRequest,
  AsyncAPIProtocolDriverSession,
} from "@openbindings/asyncapi-client";

export interface AsyncAPIKafkaDriverOptions {
  /** Exact protocol spellings this instance owns. Defaults to qualified plaintext `kafka`. */
  protocols?: readonly string[];
  /** Standalone completion for an artifact whose clientId schema has no single authored value. */
  clientId?: string;
  /** Standalone completion for an artifact whose groupId schema has no single authored value. */
  groupId?: string;
  /** Consumer offset reset used when a group has no committed offset. */
  fromBeginning?: boolean;
  /** Test/embedding seam. Ordinary consumers use Confluent's librdkafka-backed client. */
  clientFactory?: KafkaClientFactory;
}

export interface KafkaClientFactory {
  create(config: KafkaConnectionConfig): KafkaClient | Promise<KafkaClient>;
}

export interface KafkaConnectionConfig {
  brokers: readonly string[];
  clientId: string;
  sasl?: KafkaSASL;
}

export type KafkaSASL =
  | { mechanism: "plain"; username: string; password: string }
  | { mechanism: "scram-sha-256"; username: string; password: string }
  | { mechanism: "scram-sha-512"; username: string; password: string };

export interface KafkaClient {
  producer(): KafkaProducer;
  consumer(options: { groupId: string; fromBeginning: boolean }): KafkaConsumer;
}

export interface KafkaProducer {
  connect(): Promise<void>;
  send(record: { topic: string; messages: Array<{ value: Uint8Array; key?: Uint8Array }> }): Promise<unknown>;
  disconnect(): Promise<void>;
}

export interface KafkaConsumerMessage {
  value: Uint8Array | null;
  key: Uint8Array | null;
}

export interface KafkaConsumer {
  connect(): Promise<void>;
  subscribe(options: { topic: string }): Promise<void>;
  run(options: { eachMessage(message: KafkaConsumerMessage): Promise<void> }): Promise<void>;
  stop(): Promise<void>;
  disconnect(): Promise<void>;
}

/** Kafka execution under AsyncAPI Kafka binding versions 0.1.0 through 0.5.0. */
export class AsyncAPIKafkaDriver implements AsyncAPIProtocolDriver {
  readonly protocols: readonly string[];

  constructor(private readonly options: AsyncAPIKafkaDriverOptions = {}) {
    this.protocols = options.protocols ?? ["kafka"];
  }

  async execute(
    request: AsyncAPIProtocolDriverRequest,
    session: AsyncAPIProtocolDriverSession,
  ): Promise<void> {
    const profile = resolveProfile(request, this.options);
    const client = await (this.options.clientFactory ?? confluentFactory).create(profile.connection);
    if (request.action === "receive") {
      await publishInputs(client.producer(), request, session, profile);
      return;
    }
    if (!profile.groupId) {
      throw new Error("Kafka subscription requires a single authored or configured groupId");
    }
    await subscribeOutputs(
      client.consumer({ groupId: profile.groupId, fromBeginning: profile.fromBeginning }),
      request,
      session,
      profile,
    );
  }
}

export function createAsyncAPIKafkaDriver(
  options: AsyncAPIKafkaDriverOptions = {},
): AsyncAPIKafkaDriver {
  return new AsyncAPIKafkaDriver(options);
}

interface KafkaProfile {
  connection: KafkaConnectionConfig;
  topic: string;
  groupId?: string;
  key?: Uint8Array;
  fromBeginning: boolean;
}

function resolveProfile(
  request: AsyncAPIProtocolDriverRequest,
  options: AsyncAPIKafkaDriverOptions,
): KafkaProfile {
  if (
    request.operation["x-ob-asyncapi-v2-security-conjunction"] !== undefined
    || request.server?.["x-ob-asyncapi-v2-security-conjunction"] !== undefined
  ) {
    throw new Error("the Kafka profile does not admit normalized AsyncAPI 2.x multi-scheme security conjunctions");
  }
  if (request.operation["reply"] !== undefined) {
    throw new Error("the Kafka driver does not admit AsyncAPI reply operations");
  }
  const direction = request.input ?? request.output;
  if (!direction) throw new Error("Kafka request has no invocation direction");
  if (direction.address === undefined) throw new Error("Kafka request address is not resolvable before dispatch");

  const serverBinding = binding(request.server, "kafka");
  const channelBinding = binding(direction.channel, "kafka");
  const operationBinding = binding(request.operation, "kafka");
  const serverVersion = validateBinding("server", serverBinding);
  const channelVersion = validateBinding("channel", channelBinding);
  validateBinding("operation", operationBinding);
  validateServerBinding(serverBinding, serverVersion);
  validateChannelBinding(channelBinding, channelVersion);
  validateOperationBinding(operationBinding);

  const configuration = kafkaConfiguration(request);
  const topic = stringValue(channelBinding["topic"]) ?? direction.address;
  validateTopic(topic);

  let key: Uint8Array | undefined;
  for (const message of direction.messages) {
    if (message["headers"] !== undefined) {
      throw new Error("Kafka message headers are outside the payload-only OpenBindings Kafka profile");
    }
    const messageBinding = binding(message, "kafka");
    const version = validateBinding("message", messageBinding);
    validateMessageBinding(messageBinding, version);
    if (hasRegistryMessageFields(messageBinding)) {
      throw new Error("Kafka Schema Registry message framing is outside the currently qualified profile");
    }
    const authored = literalBytes(messageBinding["key"], configuration["key"], "Kafka message key");
    if (authored !== undefined) {
      if (key !== undefined && !equalBytes(key, authored)) {
        throw new Error("selected Kafka messages declare different keys");
      }
      key = authored;
    }
  }

  const groupId = literalString(
    operationBinding["groupId"],
    configuration["groupId"] ?? options.groupId,
    "Kafka groupId",
  );
  const clientId = literalString(
    operationBinding["clientId"],
    configuration["clientId"] ?? options.clientId,
    "Kafka clientId",
  ) ?? "openbindings-asyncapi";
  const fromBeginning = booleanConfiguration(configuration["fromBeginning"], options.fromBeginning ?? false);

  return {
    connection: {
      brokers: kafkaBrokers(request.serverURL),
      clientId,
      ...selectSecurity(request),
    },
    topic,
    ...(groupId ? { groupId } : {}),
    ...(key ? { key } : {}),
    fromBeginning,
  };
}

async function publishInputs(
  producer: KafkaProducer,
  request: AsyncAPIProtocolDriverRequest,
  session: AsyncAPIProtocolDriverSession,
  profile: KafkaProfile,
): Promise<void> {
  if (!request.input) throw new Error("Kafka publish request has no artifact input lane");
  await producer.connect();
  try {
    let count = 0;
    for await (const value of session.inputs) {
      if (request.signal.aborted) return;
      await producer.send({
        topic: profile.topic,
        messages: [{
          value: request.input.encode(value),
          ...(profile.key ? { key: profile.key } : {}),
        }],
      });
      count++;
    }
    if (count === 0) throw new Error("Kafka publish invocation requires at least one input value");
  } finally {
    await producer.disconnect();
  }
}

async function subscribeOutputs(
  consumer: KafkaConsumer,
  request: AsyncAPIProtocolDriverRequest,
  session: AsyncAPIProtocolDriverSession,
  profile: KafkaProfile,
): Promise<void> {
  if (!request.output) throw new Error("Kafka subscription request has no artifact output lane");
  const decodeOutput = request.output.decode;
  await session.closeInput();
  await consumer.connect();
  try {
    await consumer.subscribe({ topic: profile.topic });
    let failRun: (error: unknown) => void = () => undefined;
    const failed = new Promise<never>((_resolve, reject) => { failRun = reject; });
    void consumer.run({
      eachMessage: async (message) => {
        try {
          if (profile.key && !equalNullableBytes(message.key, profile.key)) {
            throw new Error("received Kafka record key does not match the authored message key");
          }
          if (message.value === null) {
            throw new Error("Kafka tombstone records are outside the currently qualified payload profile");
          }
          await session.emit(await decodeOutput(message.value));
        } catch (error: unknown) {
          failRun(error);
          throw error;
        }
      },
    }).catch(failRun);
    await Promise.race([untilAborted(request.signal), failed]);
  } finally {
    await consumer.stop().catch(() => undefined);
    await consumer.disconnect();
  }
}

const confluentFactory: KafkaClientFactory = {
  async create(config) {
    const { KafkaJS } = await import("@confluentinc/kafka-javascript");
    const kafka = new KafkaJS.Kafka({
      kafkaJS: {
        brokers: [...config.brokers],
        clientId: config.clientId,
        logLevel: KafkaJS.logLevel.NOTHING,
        ...(config.sasl ? { sasl: config.sasl } : {}),
      },
    });
    return {
      producer() {
        const producer = kafka.producer({
          kafkaJS: { allowAutoTopicCreation: false },
        });
        return {
          connect: () => producer.connect(),
          send: ({ topic, messages }) => producer.send({
            topic,
            messages: messages.map(({ value, key }) => ({
              value: Buffer.from(value),
              ...(key ? { key: Buffer.from(key) } : {}),
            })),
          }),
          disconnect: () => producer.disconnect(),
        };
      },
      consumer({ groupId, fromBeginning }) {
        const consumer = kafka.consumer({
          kafkaJS: {
            groupId,
            fromBeginning,
            autoCommit: true,
            allowAutoTopicCreation: false,
          },
        });
        return {
          connect: () => consumer.connect(),
          subscribe: ({ topic }) => consumer.subscribe({ topics: [topic] }),
          run: ({ eachMessage }) => consumer.run({
            eachMessage: ({ message }) => eachMessage({
              value: message.value === null ? null : new Uint8Array(message.value),
              key: message.key === null ? null : new Uint8Array(message.key),
            }),
          }),
          stop: () => consumer.stop(),
          disconnect: () => consumer.disconnect(),
        };
      },
    };
  },
};

function validateBinding(
  location: "server" | "channel" | "operation" | "message",
  value: Record<string, unknown>,
): KafkaBindingVersion {
  const version = value["bindingVersion"] ?? "0.5.0";
  if (!KAFKA_BINDING_VERSIONS.includes(version as KafkaBindingVersion)) {
    throw new Error(`Kafka ${location} binding version ${JSON.stringify(version)} is outside the 0.1.0-0.5.0 driver profile`);
  }
  return version as KafkaBindingVersion;
}

type KafkaBindingVersion = "0.1.0" | "0.2.0" | "0.3.0" | "0.4.0" | "0.5.0";
const KAFKA_BINDING_VERSIONS: readonly KafkaBindingVersion[] = ["0.1.0", "0.2.0", "0.3.0", "0.4.0", "0.5.0"];

function validateServerBinding(value: Record<string, unknown>, version: KafkaBindingVersion): void {
  validateAllowedFields(value, ["schemaRegistryUrl", "schemaRegistryVendor", "bindingVersion"], "server");
  if (version < "0.3.0" && (value["schemaRegistryUrl"] !== undefined || value["schemaRegistryVendor"] !== undefined)) {
    throw new Error(`Kafka server binding ${version} predates Schema Registry fields`);
  }
  if (value["schemaRegistryVendor"] !== undefined && value["schemaRegistryUrl"] === undefined) {
    throw new Error("Kafka schemaRegistryVendor requires schemaRegistryUrl");
  }
  if (value["schemaRegistryUrl"] !== undefined) {
    const url = stringValue(value["schemaRegistryUrl"]);
    if (!url) throw new Error("Kafka schemaRegistryUrl must be a non-empty URL string");
    try { new URL(url); } catch { throw new Error("Kafka schemaRegistryUrl must be a valid URL"); }
  }
  if (value["schemaRegistryVendor"] !== undefined && !stringValue(value["schemaRegistryVendor"])) {
    throw new Error("Kafka schemaRegistryVendor must be a non-empty string");
  }
}

function validateChannelBinding(value: Record<string, unknown>, version: KafkaBindingVersion): void {
  validateAllowedFields(value, ["topic", "partitions", "replicas", "topicConfiguration", "bindingVersion"], "channel");
  if (version < "0.3.0" && Object.keys(value).some((name) => name !== "bindingVersion")) {
    throw new Error(`Kafka channel binding ${version} must be empty`);
  }
  if (value["topic"] !== undefined) validateTopicValue(value["topic"]);
  positiveInteger(value["partitions"], "Kafka partitions");
  positiveInteger(value["replicas"], "Kafka replicas");
  if (value["topicConfiguration"] !== undefined) {
    if (version < "0.4.0") throw new Error(`Kafka channel binding ${version} predates topicConfiguration`);
    const configuration = record(value["topicConfiguration"]);
    if (!configuration) throw new Error("Kafka topicConfiguration must be an object");
    validateTopicConfiguration(configuration, version);
  }
}

function validateOperationBinding(value: Record<string, unknown>): void {
  validateAllowedFields(value, ["groupId", "clientId", "bindingVersion"], "operation");
  for (const name of ["groupId", "clientId"] as const) {
    if (value[name] !== undefined && !record(value[name])) {
      throw new Error(`Kafka ${name} must be a Schema Object`);
    }
  }
}

function validateMessageBinding(value: Record<string, unknown>, version: KafkaBindingVersion): void {
  validateAllowedFields(value, [
    "key", "schemaIdLocation", "schemaIdPayloadEncoding", "schemaLookupStrategy", "bindingVersion",
  ], "message");
  if (value["key"] !== undefined && !record(value["key"])) {
    throw new Error("Kafka message key must be a Schema Object in the qualified JSON Schema profile");
  }
  if (version < "0.3.0" && hasRegistryMessageFields(value)) {
    throw new Error(`Kafka message binding ${version} predates Schema Registry fields`);
  }
}

function validateTopicConfiguration(value: Record<string, unknown>, version: KafkaBindingVersion): void {
  const base = ["cleanup.policy", "retention.ms", "retention.bytes", "delete.retention.ms", "max.message.bytes"];
  const confluent = ["confluent.key.schema.validation", "confluent.key.subject.name.strategy", "confluent.value.schema.validation", "confluent.value.subject.name.strategy"];
  if (version === "0.4.0") validateAllowedFields(value, base, "topicConfiguration");
  if (version < "0.5.0") {
    const future = confluent.filter((name) => value[name] !== undefined);
    if (future.length > 0) throw new Error(`Kafka topicConfiguration ${version} predates fields: ${future.join(", ")}`);
  }
  if (value["cleanup.policy"] !== undefined && !["delete", "compact"].includes(String(value["cleanup.policy"]))) {
    throw new Error("Kafka cleanup.policy must be delete or compact");
  }
  for (const name of ["retention.ms", "retention.bytes", "delete.retention.ms", "max.message.bytes"]) {
    if (value[name] !== undefined && (!Number.isInteger(value[name]) || Number(value[name]) < 0)) {
      throw new Error(`Kafka ${name} must be a non-negative integer`);
    }
  }
}

function validateAllowedFields(value: Record<string, unknown>, allowed: readonly string[], location: string): void {
  const unknown = Object.keys(value).filter((name) => !allowed.includes(name)).sort();
  if (unknown.length > 0) throw new Error(`Kafka ${location} binding contains undeclared fields: ${unknown.join(", ")}`);
}

function hasRegistryMessageFields(value: Record<string, unknown>): boolean {
  return ["schemaIdLocation", "schemaIdPayloadEncoding", "schemaLookupStrategy"].some((name) => value[name] !== undefined);
}

function kafkaBrokers(serverURL: string): string[] {
  const target = new URL(serverURL);
  if (target.protocol !== "kafka:") {
    throw new Error(`Kafka driver cannot execute target scheme ${JSON.stringify(target.protocol)}`);
  }
  if ((target.pathname !== "" && target.pathname !== "/") || target.search || target.hash || target.username || target.password) {
    throw new Error("Kafka targets must contain only a broker host and optional port");
  }
  return [target.host];
}

function validateTopicValue(value: unknown): void {
  if (!stringValue(value)) throw new Error("Kafka channel topic must be a non-empty string");
  validateTopic(value as string);
}

function validateTopic(value: string): void {
  if (Buffer.byteLength(value, "utf8") > 249 || value === "." || value === ".." || !/^[a-zA-Z0-9._-]+$/.test(value)) {
    throw new Error("Kafka topic must be 1-249 bytes and contain only ASCII letters, digits, '.', '_', or '-'");
  }
}

function positiveInteger(value: unknown, name: string): void {
  if (value !== undefined && (!Number.isInteger(value) || Number(value) <= 0)) {
    throw new Error(`${name} must be a positive integer`);
  }
}

function literalString(schemaValue: unknown, configured: unknown, name: string): string | undefined {
  if (schemaValue === undefined) {
    if (configured === undefined) return undefined;
    if (!stringValue(configured)) throw new Error(`${name} configuration must be a non-empty string`);
    return configured as string;
  }
  const schema = record(schemaValue);
  if (!schema) throw new Error(`${name} must be a Schema Object`);
  const literal = schemaLiteral(schema);
  if (literal !== undefined) {
    if (!stringValue(literal)) throw new Error(`${name} authored value must be a non-empty string`);
    if (configured !== undefined && configured !== literal) throw new Error(`${name} configuration conflicts with the authored value`);
    return literal as string;
  }
  if (!stringValue(configured)) {
    throw new Error(`${name} schema does not select one value; configuration.kafka must complete it`);
  }
  validateStringSchema(configured as string, schema, name);
  return configured as string;
}

function literalBytes(schemaValue: unknown, configured: unknown, name: string): Uint8Array | undefined {
  if (schemaValue === undefined) {
    if (configured === undefined) return undefined;
    return bytesValue(configured, `${name} configuration`);
  }
  const schema = record(schemaValue);
  if (!schema) throw new Error(`${name} must be a Schema Object`);
  const literal = schemaLiteral(schema);
  const selected = literal ?? configured;
  if (selected === undefined) {
    throw new Error(`${name} schema does not select one value; configuration.kafka.key must complete it`);
  }
  if (literal !== undefined && configured !== undefined && !equalBytes(bytesValue(literal, name), bytesValue(configured, `${name} configuration`))) {
    throw new Error(`${name} configuration conflicts with the authored value`);
  }
  if (typeof selected === "string") validateStringSchema(selected, schema, name);
  return bytesValue(selected, name);
}

function schemaLiteral(schema: Record<string, unknown>): unknown {
  if (schema["const"] !== undefined) return schema["const"];
  if (schema["default"] !== undefined) return schema["default"];
  const values = schema["enum"];
  return Array.isArray(values) && values.length === 1 ? values[0] : undefined;
}

function validateStringSchema(value: string, schema: Record<string, unknown>, name: string): void {
  if (schema["type"] !== undefined && schema["type"] !== "string") throw new Error(`${name} schema must describe a string`);
  if (typeof schema["minLength"] === "number" && value.length < schema["minLength"]) throw new Error(`${name} is shorter than minLength`);
  if (typeof schema["maxLength"] === "number" && value.length > schema["maxLength"]) throw new Error(`${name} is longer than maxLength`);
  if (typeof schema["pattern"] === "string" && !new RegExp(schema["pattern"]).test(value)) throw new Error(`${name} does not match its pattern`);
  if (Array.isArray(schema["enum"]) && !schema["enum"].includes(value)) throw new Error(`${name} is outside its enum`);
}

function bytesValue(value: unknown, name: string): Uint8Array {
  if (typeof value === "string") return new TextEncoder().encode(value);
  if (value instanceof Uint8Array) return new Uint8Array(value);
  throw new Error(`${name} must be a string or Uint8Array`);
}

function selectSecurity(request: AsyncAPIProtocolDriverRequest): { sasl?: KafkaSASL } {
  if (request.securityAlternatives.length === 0) return {};
  const credential = basicCredential(request.context);
  for (const alternative of request.securityAlternatives) {
    if (alternative.length !== 1 || !credential) continue;
    const type = alternative[0]?.scheme["type"];
    if (type === "userPassword") return { sasl: { mechanism: "plain", ...credential } };
    if (type === "scramSha256") return { sasl: { mechanism: "scram-sha-256", ...credential } };
    if (type === "scramSha512") return { sasl: { mechanism: "scram-sha-512", ...credential } };
  }
  const types = [...new Set(request.securityAlternatives.flatMap((alternative) => alternative.map(({ scheme }) => String(scheme["type"]))))];
  throw new Error(`no declared AsyncAPI security alternative can be satisfied by the Kafka driver (${types.join(", ")})`);
}

function basicCredential(context: Readonly<Record<string, unknown>> | undefined): { username: string; password: string } | undefined {
  const basic = record(context?.["basic"]);
  const username = stringValue(basic?.["username"]);
  const password = stringValue(basic?.["password"]);
  return username !== undefined && password !== undefined ? { username, password } : undefined;
}

function kafkaConfiguration(request: AsyncAPIProtocolDriverRequest): Record<string, unknown> {
  const configuration = record(request.context?.["configuration"]);
  return record(configuration?.["kafka"]) ?? {};
}

function booleanConfiguration(value: unknown, fallback: boolean): boolean {
  if (value === undefined) return fallback;
  if (typeof value !== "boolean") throw new Error("configuration.kafka.fromBeginning must be boolean");
  return value;
}

function binding(owner: Readonly<Record<string, unknown>> | undefined, name: string): Record<string, unknown> {
  return record(record(owner?.["bindings"])?.[name]) ?? {};
}

function record(value: unknown): Record<string, unknown> | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined;
}

function stringValue(value: unknown): string | undefined {
  return typeof value === "string" && value !== "" ? value : undefined;
}

function equalBytes(left: Uint8Array, right: Uint8Array): boolean {
  return left.byteLength === right.byteLength && left.every((value, index) => value === right[index]);
}

function equalNullableBytes(left: Uint8Array | null, right: Uint8Array): boolean {
  return left !== null && equalBytes(left, right);
}

function untilAborted(signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return new Promise((resolve) => signal.addEventListener("abort", () => resolve(), { once: true }));
}
