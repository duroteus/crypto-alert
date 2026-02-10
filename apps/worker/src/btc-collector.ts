import { createLogger, EXCHANGES, ROUTING_KEYS } from "@alert-system/shared";
import { CircuitBreaker } from "@alert-system/shared/resilience/circuit-breaker";
import { startMetricsServer } from "@crypto-alert/shared/metrics/server";
import amqp from "amqplib";
import { randomUUID } from "crypto";

startMetricsServer(9103);

const logger = createLogger("collector");
const breaker = new CircuitBreaker(getPrice, 5, 15000);
const EXCHANGE = EXCHANGES.MAIN;
const POLL_INTERVAL = 60_000;
const FETCH_TIMEOUT = 5_000;

async function getPrice() {
  const controller = new AbortController();

  const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT);

  try {
    const res = await fetch(
      "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd",
      { signal: controller.signal },
    );

    if (res.status === 429) {
      throw new Error("RATE_LIMIT");
    }

    if (!res.ok) {
      throw new Error(`HTTP_${res.status}`);
    }

    const data = await res.json();
    return data.bitcoin.usd;
  } finally {
    clearTimeout(timeout);
  }
}

async function pollLoop(ch: amqp.Channel) {
  while (true) {
    const start = Date.now();

    try {
      const price = await breaker.execute();
      const correlationId = randomUUID();

      ch.publish(
        EXCHANGE,
        ROUTING_KEYS.PRICE_TICK,
        Buffer.from(
          JSON.stringify({
            price,
            ts: Date.now(),
          }),
        ),
        {
          persistent: true,
          headers: {
            "x-correlation-id": correlationId,
          },
        },
      );

      logger.info({ price, correlationId }, "tick published");
    } catch (err: any) {
      if (err.message === "CircuitOpen") {
        logger.warn("circuit open — skipping tick");
      } else if (err.message === "RATE_LIMIT") {
        logger.warn("coingecko rate limited");
      } else if (err.name === "AbortError") {
        logger.warn("fetch timeout");
      } else {
        logger.error({ err }, "collector failure");
      }
    }

    const elapsed = Date.now() - start;
    const sleep = Math.max(0, POLL_INTERVAL - elapsed);

    await new Promise((r) => setTimeout(r, sleep));
  }
}

async function run() {
  const conn = await amqp.connect(`amqp://${process.env.RABBITMQ_HOST}`);
  const ch = await conn.createChannel();

  await ch.assertExchange(EXCHANGE, "topic", { durable: true });

  logger.info("collector started");

  await pollLoop(ch);
}

run().catch((err) => {
  logger.fatal({ err }, "collector crashed");
  process.exit(1);
});
