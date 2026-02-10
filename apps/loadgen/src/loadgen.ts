import { EXCHANGES } from "@alert-system/shared";
import amqp from "amqplib";
import { randomUUID } from "crypto";

const EXCHANGE = EXCHANGES.MAIN;
const ROUTING_KEY = "btc.price.tick";

const RATE = 200; // msgs por segundo
const DURATION = 30_000; // ms

async function run() {
  const conn = await amqp.connect(`amqp://${process.env.RABBITMQ_HOST}`);
  const ch = await conn.createChannel();

  const interval = 1000 / RATE;

  const end = Date.now() + DURATION;

  let sent = 0;

  while (Date.now() < end) {
    const price = 60000 + Math.random() * 20000;

    ch.publish(
      EXCHANGE,
      ROUTING_KEY,
      Buffer.from(JSON.stringify({ price, ts: Date.now() })),
      {
        persistent: true,
        headers: {
          "x-correlation-id": randomUUID(),
        },
      },
    );

    sent++;

    await new Promise((r) => setTimeout(r, interval));
  }

  console.log("Sent messages:", sent);
  process.exit(0);
}

run();
