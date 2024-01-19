// Description: consommateur de l'indice de congestion moyen par tronçon
import { createConsumer, runConsumer } from './kafkaConsumer.js';

const consumer = createConsumer('consommateur_congestion', 'indice-congestion-vue');
const topic = 'indice_congestion_moyen';

const messageHandler = (key, data) => {
  console.log(`Key: ${key}    Taux de congestion: ${data.congestionIndex}    Date: ${data.datetime}`);
};

runConsumer(consumer, topic, messageHandler).catch(console.error);
