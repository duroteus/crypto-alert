import client from "prom-client";
import { register } from "./registry";

export const processingDuration = new client.Histogram({
  name: "message_processing_duration_seconds",
  help: "Processing duration",
  labelNames: ["worker"],
  buckets: [0.01, 0.05, 0.1, 0.5, 1, 2, 5],
  registers: [register],
});
