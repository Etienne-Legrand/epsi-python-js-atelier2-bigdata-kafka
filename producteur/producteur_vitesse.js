import axios from "axios";
import { Kafka, Partitioners } from "kafkajs";

const kafka = new Kafka({
  clientId: "producteur_vitesse",
  brokers: ["localhost:9092"],
});
const topic = "vitesse_moyenne_localisation";
const producer = kafka.producer({
  createPartitioner: Partitioners.DefaultPartitioner,
});

const produceMessage = async () => {
  try {
    const response = await axios.get("http://localhost:3000/api/trafic/2024-01-17T14:50:00+01:00");
    const trafficData = response.data;

    trafficData.forEach(async (troncon) => {
      const key = troncon.predefinedLocationReference;
      const averageVehicleSpeed = troncon.averageVehicleSpeed;

      console.log(`Tronçon: ${key} => Vitesse moyenne : ${averageVehicleSpeed}`);
      
      await producer.send({
        topic,
        messages: [{
          key,
          value: JSON.stringify({ averageVehicleSpeed }),
        }],
      });
    });
  } catch (error) {
    console.error("Erreur lors de la récupération des données :", error);
  }
};

const run = async () => {
  await producer.connect();
  setInterval(produceMessage, 5000);
};

run().catch(console.error);
