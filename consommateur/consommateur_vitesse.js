// Description: Consommateur Kafka pour la vitesse moyenne des véhicules par tronçon
import { createConsumer, runConsumer } from './kafkaConsumer.js';

const consumer = createConsumer('consommateur_vitesse', 'vitesse-moyenne-vue');
const topic = 'vitesse_moyenne_localisation';

const messageHandler = (key, data) => {
  console.log(`Key: ${key}    Vitesse moyenne: ${data.averageVehicleSpeed} km/h    Date: ${data.datetime}`);
};

runConsumer(consumer, topic, messageHandler).catch(console.error);