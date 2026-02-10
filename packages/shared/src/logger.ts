import pino from "pino";

export function getLoggerConfig(service: string) {
  const isDev = process.env.NODE_ENV !== "production";

  return {
    level: process.env.LOG_LEVEL || "info",

    base: {
      service,
      env: process.env.NODE_ENV || "dev",
    },

    timestamp: pino.stdTimeFunctions.isoTime,

    ...(isDev && {
      transport: {
        target: "pino-pretty",
        options: {
          colorize: true,
          translateTime: "HH:MM:ss",
          ignore: "pid,hostname",
          singleLine: false,
          errorLikeObjectKeys: ["err"],
        },
      },
    }),
  };
}

export function createLogger(service: string) {
  return pino(getLoggerConfig(service));
}
