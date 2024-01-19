import { Kafka } from "kafkajs";

const kafka = new Kafka({ clientId: 'consommateur_congestion', brokers: ['localhost:9092'] });
const topic = 'indice_congestion_moyen';
const consumer = kafka.consumer({ groupId: 'indice-congestion-vue' });

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const data = JSON.parse(message.value.toString());
      console.log(`Key: ${message.key}    Taux de congestion: ${data.congestionIndex}    Date: ${data.datetime}`);
      
      await consumer.commitOffsets([{ topic, partition, offset: message.offset }]);
    },
  });
};

run().catch(console.error);
