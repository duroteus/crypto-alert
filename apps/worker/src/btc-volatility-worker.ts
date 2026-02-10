import amqp from "amqplib";
import Redis from "ioredis";

import { createLogger, EXCHANGES, ROUTING_KEYS } from "@alert-system/shared";
import { startMetricsServer } from "@crypto-alert/shared/metrics/server";
import {
  messagesConsumed,
  messagesPublished,
  processingDuration,
  queueDepth,
  retryCount,
} from "@crypto-alert/shared/metrics";

startMetricsServer(9101);

const logger = createLogger("worker-volatility");

const redis = new Redis({
  host: process.env.REDIS_HOST,
  port: parseInt(process.env.REDIS_PORT || "6379"),
});

const EXCHANGE = EXCHANGES.MAIN;
const DLX = EXCHANGES.DLX;
const DLQ = ROUTING_KEYS.VOLATILITY_DLQ;
const RETRY_1 = ROUTING_KEYS.VOLATILITY_RETRY_1;
const RETRY_2 = ROUTING_KEYS.VOLATILITY_RETRY_2;
const PRICE_TICK = ROUTING_KEYS.PRICE_TICK;
const THRESHOLD = 0.5; // volatilidade %
const EMA_ALERT_THRESHOLD = 0.4;
const WINDOW_MS = 20 * 60 * 1000;

function classifyError(err: any) {
  const msg = err?.message ?? "";

  if (msg.includes("Simulated")) return "TRANSIENT";

  if (msg.includes("Redis")) return "INFRA";

  if (msg.includes("JSON")) return "PAYLOAD";

  return "UNKNOWN";
}

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

  // ================= RETRY INFRA =================

  await ch.assertExchange("retry.exchange", "direct", { durable: true });

  await ch.assertQueue(RETRY_1, {
    durable: true,
    arguments: {
      "x-message-ttl": 5000,
      "x-dead-letter-exchange": EXCHANGE,
      "x-dead-letter-routing-key": PRICE_TICK,
    },
  });

  await ch.assertQueue(RETRY_2, {
    durable: true,
    arguments: {
      "x-message-ttl": 30000,
      "x-dead-letter-exchange": EXCHANGE,
      "x-dead-letter-routing-key": PRICE_TICK,
    },
  });

  await ch.bindQueue(
    ROUTING_KEYS.VOLATILITY_RETRY_1,
    "retry.exchange",
    "retry.1",
  );

  await ch.bindQueue(
    ROUTING_KEYS.VOLATILITY_RETRY_2,
    "retry.exchange",
    "retry.2",
  );

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
  const queueName = q.queue;

  setInterval(async () => {
    try {
      const state = await ch.checkQueue(queueName);

      queueDepth.set({ queue: queueName }, state.messageCount);
    } catch (err) {
      logger.warn({ err }, "queue_depth_probe_failed");
    }
  }, 2000);

  await ch.bindQueue(queueName, EXCHANGE, PRICE_TICK);

  logger.info("worker_started");

  ch.consume(q.queue, async (msg) => {
    if (!msg) return;

    const correlationId = msg.properties.headers?.["x-correlation-id"];
    const log = logger.child({ correlationId });

    const routingKey = msg.fields.routingKey;

    messagesConsumed.inc({
      worker: "volatility",
      routingKey,
    });

    const endTimer = processingDuration.startTimer({
      worker: "volatility",
    });

    let stage = "start";

    try {
      if (Math.random() < 0.2) {
        throw new Error("Simulated failure");
      }

      stage = "parse";
      const { price } = JSON.parse(msg.content.toString());

      const now = Date.now();
      stage = "compute_ema";
      const emaFast = await computeEMA("btc:ema:fast", price, 9);
      const emaSlow = await computeEMA("btc:ema:slow", price, 50);

      log.debug({
        event: "dual_ema",
        emaFast,
        emaSlow,
      });

      const prevState = await redis.get("btc:ema:state");

      const currentState = emaFast > emaSlow ? "above" : "below";

      if (prevState && prevState !== currentState) {
        const type =
          currentState === "above" ? "bullish-cross" : "bearish-cross";

        log.warn({
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

        messagesPublished.inc({
          exchange: EXCHANGE,
          routingKey: "btc.alert.triggered",
        });
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

      log.info({
        event: "ema_update",
        price,
        ema,
        deviationPct,
      });

      // ===== Sliding Window =====
      stage = "volatility_window";
      await redis.zadd("btc:prices", now, JSON.stringify({ price, ts: now }));

      stage = "alerts";
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

        log.debug({
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

          messagesPublished.inc({
            exchange: EXCHANGE,
            routingKey: "btc.alert.triggered",
          });

          log.debug({
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

        messagesPublished.inc({
          exchange: EXCHANGE,
          routingKey: "btc.alert.triggered",
        });

        log.warn({
          event: "ema_alert",
          deviationPct,
          price,
          ema,
        });
      }

      ch.ack(msg);
    } catch (err) {
      const headers = msg.properties.headers || {};
      const currentRetry = headers["x-retry"] ?? 0;

      if (currentRetry < 2) {
        const route = currentRetry === 0 ? "retry.1" : "retry.2";
        const errorType = classifyError(err);

        retryCount.inc({
          worker: "volatility",
          stage,
        });

        ch.publish("retry.exchange", route, msg.content, {
          persistent: true,
          headers: {
            ...headers,
            "x-retry": currentRetry + 1,
            "x-worker": "volatility",
            "x-error-type": errorType,
            "x-error-msg": err.message,
            "x-stage": stage,
            "x-failed-at": Date.now(),
          },
        });

        log.warn(
          {
            retryCount: currentRetry,
            stage,
            errorType,
            err,
          },
          "scheduled_retry",
        );

        ch.ack(msg);
        return;
      }

      log.error(
        { retryCount: currentRetry, err },
        "retry_exhausted_sending_to_dlq",
      );

      ch.nack(msg, false, false);
    } finally {
      endTimer();
    }
  });
}

run().catch((err) => {
  logger.fatal(err, "worker_crashed");
});
