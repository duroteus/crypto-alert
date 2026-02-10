import Fastify from "fastify";
import { subscriberRoutes } from "./routes/subscribers";
import { metricsRoutes } from "./routes/metrics";
import { getLoggerConfig } from "@alert-system/shared";

const app = Fastify({
  logger: {
    ...getLoggerConfig("api"),
    serializers: {
      req(req) {
        return {
          method: req.method,
          url: req.url,
        };
      },
      res(res) {
        return {
          statusCode: res.statusCode,
        };
      },
    },
  },
});

app.register(subscriberRoutes);
app.register(metricsRoutes);

app.get("/health", async () => ({ status: "ok" }));

app.listen({
  port: 3000,
  host: "0.0.0.0",
});
