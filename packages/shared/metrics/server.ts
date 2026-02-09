import http from "http";
import { register } from "./registry";

export function startMetricsServer(port: number) {
  const server = http.createServer(async (_, res) => {
    res.setHeader("Content-Type", register.contentType);
    res.end(await register.metrics());
  });

  server.listen(port);
}
