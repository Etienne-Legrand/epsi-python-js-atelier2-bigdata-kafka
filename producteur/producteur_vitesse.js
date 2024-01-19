import axios from "axios";
import { Kafka, Partitioners } from "kafkajs";
import moment from "moment";

const kafka = new Kafka({
  clientId: "producteur_vitesse",
  brokers: ["localhost:9092"],
});
const topic = "vitesse_moyenne_localisation";
const producer = kafka.producer({
  createPartitioner: Partitioners.DefaultPartitioner,
});

const produceMessage = async (datetime) => {
  try {
    console.log(`Récupération des données pour le ${datetime}`);
    const response = await axios.get(`http://localhost:3000/api/trafic/${datetime}`);
    const trafficData = response.data;

    trafficData.forEach(async (troncon) => {
      const key = troncon.predefinedLocationReference;
      const averageVehicleSpeed = troncon.averageVehicleSpeed;

      console.log(`Tronçon: ${key}    Vitesse moyenne: ${averageVehicleSpeed} km/h    Date: ${datetime}`);
      
      await producer.send({
        topic,
        messages: [{
          key,
          value: JSON.stringify({ averageVehicleSpeed, datetime }),
        }],
      });
    });
  } catch (error) {
    console.error("Erreur lors de la récupération des données :", error);
  }
};

const run = async () => {
  await producer.connect();

  const startTime = moment("2024-01-17T14:09:00+01:00");
  const endTime = moment("2024-01-17T14:50:00+01:00");
  let currentTime = moment(startTime);

  // Itérer sur chaque minute entre startTime et endTime pour parcourir les données
  // et envoyer un message à Kafka
  while (currentTime <= endTime) {
    await produceMessage(currentTime.format());
    currentTime.add(1, 'minute');
    await new Promise(resolve => setTimeout(resolve, 1000));
  }

  await producer.disconnect();
};

run().catch(console.error);
