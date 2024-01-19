import { Kafka } from "kafkajs";

const mem = { A: 0, B: 0, C: 0, D: 0, E: 0, F: 0 };

const kafka = new Kafka({clientId: 'my-consumer', brokers: ['localhost:9092']});
const topic = 'triplets';
const consumer = kafka.consumer({ groupId: 'triplets-vue' });

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const nombres = JSON.parse(message.value.toString());
      mem[message.key] = nombres.val1;
      console.log(Object.entries(mem).map(([cle, valeur]) => `${cle}: ${valeur}`).join(', '));
    },
  });
};

run().catch(console.error);