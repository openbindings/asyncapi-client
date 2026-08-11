import { createServer } from "node:net";
import { Aedes } from "aedes";

const broker = await Aedes.createBroker();
broker.authenticate = (_client, username, password, done) => {
  done(null, username === "sensor" && password?.toString() === "secret");
};
const partialFailureClients = new WeakSet();
broker.on("subscribe", (subscriptions, client) => {
  if (partialFailureClients.has(client) || !subscriptions.some(({ topic }) => topic === "failure/acme")) return;
  partialFailureClients.add(client);
  broker.publish({
    cmd: "publish",
    topic: "failure/acme",
    payload: Buffer.from(JSON.stringify({ id: "before-disconnect" })),
    qos: 1,
    dup: false,
    retain: false,
  }, (error) => {
    if (error) {
      client.emit("error", error);
      return;
    }
    setTimeout(() => client.conn.destroy(), 50);
  });
});
const server = createServer(broker.handle);
await new Promise((resolve, reject) => {
  server.once("error", reject);
  server.listen(0, "127.0.0.1", resolve);
});
const address = server.address();
if (!address || typeof address === "string") throw new Error("MQTT broker has no TCP address");
process.stdout.write(`mqtt://127.0.0.1:${address.port}\n`);

let closing = false;
async function close() {
  if (closing) return;
  closing = true;
  await new Promise((resolve) => server.close(resolve));
  await new Promise((resolve) => broker.close(resolve));
  process.exit(0);
}
process.on("SIGTERM", () => { void close(); });
process.on("SIGINT", () => { void close(); });
