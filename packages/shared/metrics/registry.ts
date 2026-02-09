import client from "prom-client";

export const register = (global as any).__PROM_REG__ ?? new client.Registry();

(global as any).__PROM_REG__ = register;

client.collectDefaultMetrics({
  register,
});
