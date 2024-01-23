import { createProducer, fetchData, runProducer } from './kafkaProducer.js';

const producer = createProducer("producteur_vitesse");
const topic = "vitesse_moyenne_localisation";

const produceMessage = async (datetime) => {
  try {
    const trafficData = await fetchData(datetime);

    trafficData.forEach(async (troncon) => {
      const key = troncon.predefinedLocationReference;
      const averageVehicleSpeed = troncon.averageVehicleSpeed;

      console.log(`Tronçon: ${key}    Vitesse moyenne: ${averageVehicleSpeed} km/h    Date: ${datetime}`);

      await producer.send({
        topic,
        messages: [
          {
            key,
            value: JSON.stringify({ key, averageVehicleSpeed, datetime }),
          },
        ],
      });
    });
  } catch (error) {
    console.error("Erreur lors de la récupération des données :", error);
  }
};

runProducer(producer, produceMessage).catch(console.error);