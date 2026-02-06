import { EXCHANGES, ROUTING_KEYS } from "@alert-system/shared";
import amqp from "amqplib";
import { randomUUID } from "crypto";

const EXCHANGE = EXCHANGES.MAIN;

async function getPrice() {
  const res = await fetch(
    "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd",
  );

  if (res.status === 429) {
    throw new Error("Rate limit exceeded");
  }

  if (!res.ok) {
    throw new Error("Failed to fetch price");
  }

  const data = await res.json();
  return data.bitcoin.usd;
}

async function run() {
  const conn = await amqp.connect(`amqp://${process.env.RABBITMQ_HOST}`);
  const ch = await conn.createChannel();

  await ch.assertExchange(EXCHANGE, "topic", { durable: true });

  console.log("📡 Coletor BTC iniciado");

  setInterval(async () => {
    try {
      const price = await getPrice();
      const correlationId = randomUUID();

      ch.publish(
        EXCHANGE,
        ROUTING_KEYS.PRICE_TICK,
        Buffer.from(
          JSON.stringify({
            correlationId,
            price,
            ts: Date.now(),
          }),
        ),
      );

      console.log("Tick:", price, "correlationId:", correlationId);
    } catch (err) {
      console.error("Erro coletando preço", err);
    }
  }, 60_000);
}

run().catch(console.error);
