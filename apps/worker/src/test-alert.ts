import amqp from "amqplib";

async function run() {
  const conn = await amqp.connect(`amqp://${process.env.RABBITMQ_HOST}`);
  const ch = await conn.createChannel();

  await ch.assertExchange("crypto.topic", "topic", { durable: true });

  ch.publish(
    "crypto.topic",
    "btc.alert.triggered",
    Buffer.from(
      JSON.stringify({
        variation: 5.2,
        price: 64000,
      }),
    ),
  );

  console.log("Alerta publicado");

  await ch.close();
  await conn.close();
}

run();
