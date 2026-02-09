import client from "prom-client";
import { register } from "./registry";

export const messagesConsumed = new client.Counter({
  name: "messages_consumed_total",
  help: "Total consumed messages",
  labelNames: ["worker", "routingKey"],
  registers: [register],
});

export const messagesPublished = new client.Counter({
  name: "messages_published_total",
  help: "Total published messages",
  labelNames: ["exchange", "routingKey"],
  registers: [register],
});

export const retryCount = new client.Counter({
  name: "retry_attempt_total",
  help: "Retry attempts",
  labelNames: ["worker", "stage"],
  registers: [register],
});
