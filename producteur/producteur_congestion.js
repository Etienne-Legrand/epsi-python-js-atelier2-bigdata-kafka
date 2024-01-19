import axios from "axios";
import { Kafka, Partitioners } from "kafkajs";

const kafka = new Kafka({
  clientId: "my-producer",
  brokers: ["localhost:9092"],
});
const topic = "indice_congestion_moyen";
const producer = kafka.producer({
  createPartitioner: Partitioners.DefaultPartitioner,
});

const calculateCongestionIndex = (troncon) => {
  let totalTroncons = 1; // Initialisé à 1 car nous avons au moins un tronçon
  let totalCongested = 0;

  switch (troncon.trafficStatus) {
    case "congested":
      totalCongested = 0.6;
    case "heavy":
      totalCongested = 0.8;
    case "impossible":
      totalCongested = 1;
      break;
    default:
      totalCongested = 0;
  }

  const congestionIndex = (totalCongested / totalTroncons) * 100;

  return congestionIndex;
};

const produceMessage = async () => {
  try {
    const response = await axios.get("http://localhost:3000/api/trafic/2024-01-17T14:50:00+01:00");
    const trafficData = response.data;

    if (Array.isArray(trafficData)) {
      trafficData.forEach(async (troncon) => {
        const key = troncon.predefinedLocationReference;
        const congestionIndex = calculateCongestionIndex(troncon);

        console.log(`KEY: ${key} Taux de congestion : ${congestionIndex}`);
        
        await producer.send({
          topic,
          messages: [{
            key,
            value: JSON.stringify({ congestionIndex }),
          }],
        });
      });
    }
  } catch (error) {
    console.error("Erreur lors de la récupération des données :", error);
  }
};

const run = async () => {
  await producer.connect();
  setInterval(produceMessage, 10000);
};

run().catch(console.error);
