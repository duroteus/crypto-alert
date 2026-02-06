import { FastifyInstance } from "fastify";
import { pool } from "../db";
import { getChannel } from "../events";
import { EXCHANGES, ROUTING_KEYS } from "@alert-system/shared";
import subscriber from "../models/subscriber";

export async function subscriberRoutes(app: FastifyInstance) {
  app.get("/subscribers", async () => {
    const { rows } = await pool.query(
      "SELECT chat_id FROM subscribers WHERE active=true",
    );

    return rows;
  });

  app.post("/subscribers", async (req, reply) => {
    const { chatId } = req.body as { chatId: string };

    const ch = await getChannel();

    ch.publish(
      EXCHANGES.MAIN,
      ROUTING_KEYS.SUBSCRIBER_CREATED,
      Buffer.from(JSON.stringify({ chatId })),
    );

    await subscriber.create(chatId);

    return { ok: true };
  });

  app.delete("/subscribers/:chatId", async (req) => {
    const { chatId } = req.params as { chatId: string };

    const ch = await getChannel();
    ch.publish(
      EXCHANGES.MAIN,
      ROUTING_KEYS.SUBSCRIBER_REMOVED,
      Buffer.from(JSON.stringify({ chatId })),
    );

    await subscriber.inactivate(chatId);

    return { ok: true };
  });
}
