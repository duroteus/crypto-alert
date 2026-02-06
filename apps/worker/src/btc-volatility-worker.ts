import amqp from "amqplib";
import Redis from "ioredis";

import { createLogger, EXCHANGES, ROUTING_KEYS } from "@alert-system/shared";

const logger = createLogger("worker-volatility");

const redis = new Redis({
  host: process.env.REDIS_HOST,
  port: parseInt(process.env.REDIS_PORT || "6379"),
});

const EXCHANGE = EXCHANGES.MAIN;
const DLX = EXCHANGES.DLX;
const DLQ = "btc.volatility.dlq";
const THRESHOLD = 0.5; // volatilidade %
const EMA_ALERT_THRESHOLD = 0.4;

async function computeEMA(key: string, price: number, window: number) {
  const k = 2 / (window + 1);

  const prev = await redis.get(key);
  const prevEma = prev ? Number(prev) : price;

  const ema = price * k + prevEma * (1 - k);

  await redis.set(key, ema);

  return ema;
}

async function run() {
  const conn = await amqp.connect(`amqp://${process.env.RABBITMQ_HOST}`);
  const ch = await conn.createChannel();

  await ch.assertExchange(DLX, "topic", { durable: true });
  await ch.assertQueue(DLQ, { durable: true });
  await ch.bindQueue(DLQ, DLX, "#");

  await ch.assertExchange(EXCHANGE, "topic", { durable: true });
  const q = await ch.assertQueue("btc.volatility", {
    durable: true,
    arguments: {
      "x-dead-letter-exchange": DLX,
    },
  });
  await ch.bindQueue(q.queue, EXCHANGE, ROUTING_KEYS.PRICE_TICK);

  logger.info("worker_started");

  const WINDOW_MS = 20 * 60 * 1000;

  ch.consume(q.queue, async (msg) => {
    if (!msg) return;

    try {
      if (Math.random() < 0.2) {
        throw new Error("Simulated failure");
      }

      const { price, correlationId } = JSON.parse(msg.content.toString());
      const now = Date.now();
      const emaFast = await computeEMA("btc:ema:fast", price, 5);
      const emaSlow = await computeEMA("btc:ema:slow", price, 20);

      logger.debug({
        correlationId,
        event: "dual_ema",
        emaFast,
        emaSlow,
      });

      const prevState = await redis.get("btc:ema:state");

      const currentState = emaFast > emaSlow ? "above" : "below";

      if (prevState && prevState !== currentState) {
        const type =
          currentState === "above" ? "bullish-cross" : "bearish-cross";

        logger.warn({
          correlationId,
          event: "ema_crossover",
          type,
          price,
          emaFast,
          emaSlow,
        });

        ch.publish(
          EXCHANGE,
          "btc.alert.triggered",
          Buffer.from(
            JSON.stringify({
              correlationId,
              type,
              price,
              emaFast,
              emaSlow,
            }),
          ),
        );
      }

      await redis.set("btc:ema:state", currentState);

      // ===== EMA =====
      const EMA_KEY = "btc:ema";
      const EMA_WINDOW = 10;

      const k = 2 / (EMA_WINDOW + 1);
      const prev = await redis.get(EMA_KEY);
      const prevEma = prev ? Number(prev) : price;

      const ema = price * k + prevEma * (1 - k);
      await redis.set(EMA_KEY, ema);

      const deviationPct = ((price - ema) / ema) * 100;

      logger.info({
        correlationId,
        event: "ema_update",
        price,
        ema,
        deviationPct,
      });

      // ===== Sliding Window =====
      await redis.zadd("btc:prices", now, JSON.stringify({ price, ts: now }));

      const cutoff = now - WINDOW_MS;
      await redis.zremrangebyscore("btc:prices", 0, cutoff);

      const entries = await redis.zrange("btc:prices", 0, -1);
      const prices = entries.map((e) => JSON.parse(e).price);

      if (prices.length >= 2) {
        const avg = prices.reduce((a, b) => a + b, 0) / prices.length;

        const variance =
          prices.reduce((sum, p) => sum + (p - avg) ** 2, 0) / prices.length;

        const std = Math.sqrt(variance);
        const varPct = (std / avg) * 100;

        logger.debug({
          correlationId,
          event: "volatility_window",
          window: prices.length,
          volatility: varPct,
        });

        // ===== Volatility Alert =====
        if (Math.abs(varPct) >= THRESHOLD) {
          ch.publish(
            EXCHANGE,
            "btc.alert.triggered",
            Buffer.from(
              JSON.stringify({
                correlationId,
                type: "volatility",
                price,
                volatility: varPct,
                windowSize: prices.length,
              }),
            ),
          );

          logger.debug({
            correlationId,
            event: "volatility_calc",
            window: prices.length,
            volatility: varPct,
          });
        }
      }

      // ===== EMA Alert =====
      if (Math.abs(deviationPct) >= EMA_ALERT_THRESHOLD) {
        ch.publish(
          EXCHANGE,
          "btc.alert.triggered",
          Buffer.from(
            JSON.stringify({
              correlationId,
              type: "ema-deviation",
              price,
              ema,
              deviation: deviationPct,
            }),
          ),
        );

        logger.warn({
          correlationId,
          event: "ema_alert",
          deviationPct,
          price,
          ema,
        });
      }

      ch.ack(msg);
    } catch (err) {
      logger.error(err, "processing_failed");
      ch.nack(msg, false, false);
    }
  });
}

run().catch((err) => {
  logger.fatal(err, "worker_crashed");
});
