import client from "prom-client";
import { register } from "./registry";

export const activeSubscribers = new client.Gauge({
  name: "subscribers_active",
  help: "Number of active subscribers",
  registers: [register],
});

export const circuitBreakerState = new client.Gauge({
  name: "circuit_breaker_state",
  help: "Circuit breaker state (0=closed,1=open,2=half)",
  labelNames: ["service"],
  registers: [register],
});

export const workerHeartbeat = new client.Gauge({
  name: "worker_heartbeat",
  help: "Worker heartbeat timestamp",
  labelNames: ["worker"],
  registers: [register],
});

export const queueDepth = new client.Gauge({
  name: "rabbitmq_queue_depth",
  help: "Number of messages ready in queue",
  labelNames: ["queue"],
  registers: [register],
});
