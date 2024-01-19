import express from "express";
import fs from "fs";
import { parse } from "csv-parse";
const PORT = 3000;

// Start express
const app = express();
app.use(express.json());

// CORS Settings
app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  next();
});

// Router API
const router = express.Router();
app.use("/api", router);

// Route pour récupérer les données de trafic pour une date donnée
router.get("/trafic/:datetime", (req, res) => {
  const requestedDatetime = req.params.datetime;
  const data = [];

  fs.createReadStream("./datamerge.csv")
    .pipe(parse({ delimiter: ";", from_line: 2 }))
    .on("data", function (row) {
      const rowData = {
        datetime: row[0],
        predefinedLocationReference: row[1],
        averageVehicleSpeed: row[2],
        travelTime: row[3],
        travelTimeReliability: row[4],
        trafficStatus: row[5],
        vehicleProbeMeasurement: row[6],
        GeoPoint: row[7],
        GeoShape: row[8],
        gml_id: row[9],
        id_rva_troncon_fcd_v1_1: row[10],
        hierarchie: row[11],
        hierarchie_dv: row[12],
        denomination: row[13],
        insee: row[14],
        sens_circule: row[15],
        vitesse_maxi: row[16],
      };
      data.push(rowData);
    })
    .on("end", function () {
      // Filtrer les données par date
      const filteredData = data.filter(
        (entry) => entry.datetime === requestedDatetime
      );

      // Envoyer les données filtrées en tant que réponse JSON
      res.json(filteredData);
    })
    .on("error", function (error) {
      console.log(error.message);
      res
        .status(500)
        .json({ error: "Erreur lors de la lecture du fichier CSV" });
    });
});

// Port d'écoute
app.listen(PORT, () => {
  console.log(`Le serveur est lancé sur le port ${PORT}`);
});
