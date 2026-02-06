import pino from "pino";

export function createLogger(service: string) {
  return pino({
    level: process.env.LOG_LEVEL || "info",

    base: {
      service,
      env: process.env.NODE_ENV || "dev",
    },

    timestamp: pino.stdTimeFunctions.isoTime,
  });
}
