import { createProducer, fetchData, runProducer } from './kafkaProducer.js';

const producer = createProducer("producteur_congestion");
const topic = "indice_congestion_moyen";

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
    const trafficData = await fetchData(datetime);

    trafficData.forEach(async (troncon) => {
      const key = troncon.predefinedLocationReference;
      const congestionIndex = calculateCongestionIndex(troncon);

      console.log(`Tronçon: ${key}    Taux de congestion: ${congestionIndex}    Date: ${datetime}`);

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

runProducer(producer, produceMessage).catch(console.error);