import { EXCHANGES } from "@alert-system/shared";
import amqp from "amqplib";

let channel: amqp.Channel | null = null;

export async function getChannel() {
  if (channel) return channel;

  const conn = await amqp.connect(`amqp://${process.env.RABBITMQ_HOST}`);
  channel = await conn.createChannel();

  await channel.assertExchange(EXCHANGES.MAIN, "topic", { durable: true });

  return channel;
}
