import axios from "axios";
import { Kafka, Partitioners } from "kafkajs";

const kafka = new Kafka({
  clientId: "producteur_congestion",
  brokers: ["localhost:9092"],
});
const topic = "indice_congestion_moyen";
const producer = kafka.producer({
  createPartitioner: Partitioners.DefaultPartitioner,
});

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

const produceMessage = async () => {
  try {
    const response = await axios.get("http://localhost:3000/api/trafic/2024-01-17T14:50:00+01:00");
    const trafficData = response.data;

    trafficData.forEach(async (troncon) => {
      const key = troncon.predefinedLocationReference;
      const congestionIndex = calculateCongestionIndex(troncon);

      console.log(`Tronçon: ${key} => Taux de congestion: ${congestionIndex}`);
      
      await producer.send({
        topic,
        messages: [{
          key,
          value: JSON.stringify({ congestionIndex }),
        }],
      });
    });
  } catch (error) {
    console.error("Erreur lors de la récupération des données :", error);
  }
};

const run = async () => {
  await producer.connect();
  setInterval(produceMessage, 1000);
};

run().catch(console.error);
