import Fastify from "fastify";
import { subscriberRoutes } from "./routes/subscribers";

const app = Fastify({
  logger: true,
});

app.register(subscriberRoutes);

app.get("/health", async () => ({ status: "ok" }));

app.listen({
  port: 3000,
  host: "0.0.0.0",
});
