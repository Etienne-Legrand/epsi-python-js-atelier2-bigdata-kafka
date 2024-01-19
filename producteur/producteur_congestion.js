// Importation des modules nécessaires
import axios from "axios";
import { Kafka, Partitioners } from "kafkajs";
import moment from "moment";

// Configuration de l'objet Kafka pour la connexion au broker Kafka
const kafka = new Kafka({clientId: "producteur_congestion", brokers: ["localhost:9092"]});
const topic = "indice_congestion_moyen";
const producer = kafka.producer({createPartitioner: Partitioners.DefaultPartitioner});

// Calcul de l'indice de congestion en fonction du statut de trafic d'un tronçon
const calculateCongestionIndex = (troncon) => {
  let congestionIndex = 0;

  switch (troncon.trafficStatus) {
    case "congested":
      congestionIndex = 0.6;
      break;
    case "heavy":
      congestionIndex = 0.8;
      break;
    case "impossible":
      congestionIndex = 1;
      break;
  }

  return congestionIndex;
};

// Envoie un message Kafka en récupérant les données de trafic à partir de l'API
const produceMessage = async (datetime) => {
  try {
    console.log(`Récupération des données pour le ${datetime}`);
    const response = await axios.get(`http://localhost:3000/api/trafic/${datetime}`);
    const trafficData = response.data;

    trafficData.forEach(async (troncon) => {
      const key = troncon.predefinedLocationReference;
      const congestionIndex = calculateCongestionIndex(troncon);

      console.log(`Tronçon: ${key}, Taux de congestion: ${congestionIndex}, Date: ${datetime}`);

      await producer.send({
        topic,
        messages: [{
            key,
            value: JSON.stringify({ congestionIndex, datetime }),
          }],
      });
    });
  } catch (error) {
    console.error("Erreur lors de la récupération des données :", error);
  }
};

// Fonction principale pour exécuter le processus d'envoi de messages à Kafka
// en parcourant les données de trafic entre deux dates
const run = async () => {
  await producer.connect();

  const startTime = moment("2024-01-17T14:09:00+01:00");
  const endTime = moment("2024-01-17T14:50:00+01:00");
  let currentTime = moment(startTime);

  // Itération sur chaque minute entre startTime et endTime pour parcourir les données
  // et envoyer un message à Kafka
  while (currentTime <= endTime) {
    await produceMessage(currentTime.format());
    currentTime.add(1, 'minute');
    await new Promise(resolve => setTimeout(resolve, 1000));
  }

  await producer.disconnect();
};

run().catch(console.error);
