import amqp from "amqplib";
import Redis from "ioredis";

import { createLogger, EXCHANGES, ROUTING_KEYS } from "@alert-system/shared";

const logger = createLogger("worker-telegram");

const redis = new Redis({
  host: process.env.REDIS_HOST,
  port: parseInt(process.env.REDIS_PORT || "6379"),
});

const TOKEN = process.env.TELEGRAM_TOKEN;

const EXCHANGE = EXCHANGES.MAIN;
const ROUTING_KEY = ROUTING_KEYS.ALERT_TRIGGERED;

// ==========================
// 🛠️ Helpers
// ==========================

function formatPrice(p: number) {
  return `$${p.toFixed(2)}`;
}

function severityVol(v: number) {
  if (v >= 1) return { label: "HIGH", icon: "🔴" };
  if (v >= 0.5) return { label: "MEDIUM", icon: "🟠" };
  return { label: "LOW", icon: "🟡" };
}

function severityEma(v: number) {
  const a = Math.abs(v);

  if (a >= 0.6) return { label: "HIGH", icon: "🔴" };
  if (a >= 0.3) return { label: "MEDIUM", icon: "🟠" };
  return { label: "LOW", icon: "🟡" };
}

async function sendTelegram(chatId: string, text: string) {
  await fetch(`https://api.telegram.org/bot${TOKEN}/sendMessage`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      chat_id: chatId,
      text,
      parse_mode: "Markdown",
    }),
  });
}

async function fetchSubscribers() {
  const res = await fetch(`${process.env.API_HOST}/subscribers`);
  return res.json();
}

async function shouldSendAlert(type: string, severity: string) {
  // LOW nunca envia
  if (severity === "LOW") return false;

  // HIGH sempre envia
  if (severity === "HIGH") return true;

  // MEDIUM → cooldown
  const key = `cooldown:btc:${type}`;

  const exists = await redis.get(key);
  if (exists) return false;

  await redis.set(key, "1", "EX", 600); // 10 min
  return true;
}

// ==========================
// 📲 Telegram Worker
// ==========================

async function run() {
  const conn = await amqp.connect(`amqp://${process.env.RABBITMQ_HOST}`);
  const ch = await conn.createChannel();

  await ch.assertExchange(EXCHANGE, "topic", { durable: true });

  const q = await ch.assertQueue("", { exclusive: true });
  await ch.bindQueue(q.queue, EXCHANGE, ROUTING_KEY);

  console.log("📲 Telegram worker ouvindo alertas...");

  ch.consume(q.queue, async (msg) => {
    if (!msg) return;

    const payload = JSON.parse(msg.content.toString());
    const { correlationId } = payload;

    let text = "";

    switch (payload.type) {
      case "volatility":
        const sevVol = severityVol(payload.volatility);

        if (!(await shouldSendAlert("volatility", sevVol.label))) {
          console.log("⏸️ Cooldown ativo (VOL)");
          ch.ack(msg);
          return;
        }

        text = `
        ${sevVol.icon} *Movimento incomum no preço do Bitcoin*

        O mercado está oscilando mais do que o normal neste momento.

        • Preço atual: ${formatPrice(payload.price)}
        • Nível de oscilação: ${payload.volatility.toFixed(3)}%

        Isso não é necessariamente bom ou ruim — apenas indica instabilidade no curto prazo.
        `.trim();
        break;

      case "ema-deviation":
        const sevEma = severityEma(payload.deviation);

        if (!(await shouldSendAlert("ema", sevEma.label))) {
          console.log("⏸️ Cooldown ativo (EMA)");
          ch.ack(msg);
          return;
        }

        const direction = payload.deviation > 0 ? "acima" : "abaixo";

        text = `
        ${sevEma.icon} *Mudança recente no comportamento do preço*

        O preço do Bitcoin está ${direction} do padrão recente.

        • Preço atual: ${formatPrice(payload.price)}
        • Tendência recente: ${formatPrice(payload.ema)}

        Isso pode indicar início de um movimento de alta ou baixa.
        `.trim();
        break;

      case "bullish-cross":
        text = `
      🟢 *Possível início de tendência de alta*

      Detectamos sinais de que o preço pode estar entrando em um movimento positivo.

      • Preço atual: ${formatPrice(payload.price)}

      Isso não é recomendação financeira — apenas um indicador estatístico.
      `.trim();
        break;
      case "bearish-cross":
        text = `
      🔴 *Possível início de tendência de baixa*

      Detectamos sinais de enfraquecimento do preço.

      • Preço atual: ${formatPrice(payload.price)}

      Isso não é garantia de queda — apenas um sinal baseado em padrões recentes.
      `.trim();
        break;

      default:
        text = `⚠️ Unknown alert:\n${JSON.stringify(payload)}`;
        break;
    }

    text += `

    ━━━━━━━━━━━━
    ℹ️ Alertas automáticos baseados em análise estatística
    `;

    const subscribers = await fetchSubscribers();

    for (const sub of subscribers) {
      await sendTelegram(sub.chat_id, text);
    }

    logger.info({
      event: "telegram_broadcast",
      count: subscribers.length,
    });

    ch.ack(msg);
  });
}

run().catch(console.error);
