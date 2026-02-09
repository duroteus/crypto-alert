import { register } from "@crypto-alert/shared/metrics";
import { FastifyInstance } from "fastify";

export async function metricsRoutes(app: FastifyInstance) {
  app.get("/metrics", async (_, reply) => {
    reply.header("Content-Type", register.contentType);
    return register.metrics();
  });
}
