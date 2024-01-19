import axios from "axios";
import { Kafka, Partitioners } from "kafkajs";

const kafka = new Kafka({
  clientId: "my-producer",
  brokers: ["localhost:9092"],
});
const topic = "vitesse_moyenne_localisation";
const producer = kafka.producer({
  createPartitioner: Partitioners.DefaultPartitioner,
});

// Structure pour stocker temporairement les données
const speedData = {};

const calculateAverageSpeed = (speeds) => {
  if (speeds.length === 0) return 0;

  const totalSpeed = speeds.reduce((acc, speed) => acc + speed, 0);
  const averageSpeed = totalSpeed / speeds.length;

  return averageSpeed;
};

const groupByPredefinedLocation = (trafficData) => {
  const groupedData = {};
  trafficData.forEach((troncon) => {
    const key = troncon.predefinedLocationReference;
    if (!groupedData[key]) {
      groupedData[key] = [];
    }
    groupedData[key].push(troncon.averageVehicleSpeed || 0);
  });
  return groupedData;
};

const produceMessage = async () => {
  try {
    const response = await axios.get("http://localhost:3000/api/trafic/2024-01-17T14:50:00+01:00");
    const trafficData = response.data;

    if (Array.isArray(trafficData)) {
      const groupedData = groupByPredefinedLocation(trafficData);

      Object.keys(groupedData).forEach(async (key) => {
        const averageSpeed = calculateAverageSpeed(groupedData[key]);

        console.log(`KEY: ${key} Vitesse moyenne : ${averageSpeed}`);
        
        await producer.send({
          topic,
          messages: [{
            key,
            value: JSON.stringify({ averageSpeed }),
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
  setInterval(produceMessage, 1000);
};

run().catch(console.error);
