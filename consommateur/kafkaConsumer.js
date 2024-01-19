// Description: Fonction permettant de créer un consommateur Kafka et de consommer des messages.
import { Kafka } from "kafkajs";

/**
 * Crée un consommateur Kafka.
 * @param {string} clientId - L'identifiant du client.
 * @param {string} groupId - L'identifiant du groupe.
 * @returns {Object} Le consommateur Kafka.
 */
export const createConsumer = (clientId, groupId) => {
  const kafka = new Kafka({ clientId, brokers: ['localhost:9092'] });
  return kafka.consumer({ groupId });
};

/**
 * Connecte le consommateur, s'abonne à un topic et commence à consommer des messages.
 * @param {Object} consumer - Le consommateur Kafka.
 * @param {string} topic - Le topic auquel s'abonner.
 * @param {Function} messageHandler - La fonction à appeler pour chaque message consommé.
 */
export const runConsumer = async (consumer, topic, messageHandler) => {
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const data = JSON.parse(message.value.toString());
      await messageHandler(message.key, data);
      await consumer.commitOffsets([{ topic, partition, offset: message.offset }]);
    },
  });
};