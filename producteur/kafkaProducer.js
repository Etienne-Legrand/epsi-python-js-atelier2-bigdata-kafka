// Description: Fonctions pour créer un producteur Kafka et envoyer des messages à Kafka
import axios from "axios";
import { Kafka, Partitioners } from "kafkajs";
import moment from "moment";

// Création d'un producteur Kafka
export const createProducer = (clientId) => {
  const kafka = new Kafka({clientId, brokers: ["localhost:9092"]});
  return kafka.producer({createPartitioner: Partitioners.DefaultPartitioner});
};

// Récupération des données de trafic pour une date et une heure données
export const fetchData = async (datetime) => {
  console.log(`Récupération des données pour le ${datetime}`);
  const response = await axios.get(`http://localhost:3000/api/trafic/${datetime}`);
  return response.data;
};

// Fonction principale pour exécuter le processus d'envoi de messages à Kafka
// en parcourant les données de trafic entre deux dates
export const runProducer = async (producer, produceMessage) => {
  await producer.connect();

  const startTime = moment("2024-01-17T14:09:00+01:00");
  const endTime = moment("2024-01-17T14:50:00+01:00");
  let currentTime = moment(startTime);

  while (currentTime <= endTime) {
    await produceMessage(currentTime.format());
    currentTime.add(1, "minute");
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }

  await producer.disconnect();
};