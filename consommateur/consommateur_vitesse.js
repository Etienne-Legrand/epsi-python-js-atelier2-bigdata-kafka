import { Kafka } from "kafkajs";

const kafka = new Kafka({ clientId: 'consommateur_vitesse', brokers: ['localhost:9092'] });
const topic = 'vitesse_moyenne_localisation';
const consumer = kafka.consumer({ groupId: 'vitesse-moyenne-vue' });

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const data = JSON.parse(message.value.toString());
      console.log(`Key: ${message.key} => Vitesse moyenne: ${data.averageSpeed}`);
      
      await consumer.commitOffsets([{ topic, partition, offset: message.offset }]);
    },
  });
};

run().catch(console.error);
