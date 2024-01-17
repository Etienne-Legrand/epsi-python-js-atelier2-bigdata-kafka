const express = require("express");
const fs = require("fs");
const { parse } = require("csv-parse");

const PORT = 3000;

// Fichier CSV
const db = new Datastore({ filename: "movies" });
db.loadDatabase();

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

// Port d'écoute
app.listen(PORT, () => {
  console.log(`Le serveur est lancé sur le port ${PORT}`);
});

/**************************************
 *********** API CRUD MOVIES **********
 **************************************/

// Create
router.post("/movies", (req, res) => {
  // Vérifier si toutes les propriétés nécessaires sont présentes
  const requiredProps = [
    "titre",
    "anneeDeSortie",
    "langue",
    "realisateur",
    "genre",
    "poster",
  ];
  const missingProps = requiredProps.filter(
    (prop) => !req.body.hasOwnProperty(prop)
  );

  if (missingProps.length > 0) {
    res.status(400).json({
      error: `Les propriétés suivantes sont manquantes : ${missingProps.join(
        ", "
      )}`,
    });
  } else {
    const movie = req.body;
    db.insert(movie, (err, newMovie) => {
      if (err) {
        res.status(500).json({
          error: "Une erreur est survenue lors de la création du film.",
        });
      } else {
        res.status(201).json(newMovie);
      }
    });
  }
});

// Read all
router.get("/movies", (req, res) => {
  db.find({}, (err, docs) => {
    if (err) {
      res.status(500).json({
        error: "Une erreur est survenue lors de la récupération des films.",
      });
    } else {
      res.status(200).json(docs);
    }
  });
});

// Read one
router.get("/movies/:id", (req, res) => {
  db.findOne({ _id: req.params.id }, (err, movie) => {
    if (err) {
      res.status(500).json({
        error: "Une erreur est survenue lors de la récupération du film.",
      });
    } else if (!movie) {
      res.status(404).json({ error: "Aucun film trouvé avec cet ID." });
    } else {
      res.status(200).json(movie);
    }
  });
});

// Update
router.patch("/movies/:id", (req, res) => {
  const updatedMovie = { ...req.body };

  db.update(
    { _id: req.params.id },
    { $set: updatedMovie },
    {},
    (err, nbMoviesUpdated) => {
      if (err) {
        res.status(500).json({
          error: "Une erreur est survenue lors de la mise à jour du film.",
        });
      } else if (nbMoviesUpdated === 0) {
        res.status(404).json({ error: "Aucun film trouvé avec cet ID." });
      } else {
        res.status(200).json(updatedMovie);
      }
    }
  );
});

// Delete
router.delete("/movies/:id", (req, res) => {
  db.remove({ _id: req.params.id }, {}, (err, nbMoviesRemoved) => {
    if (err) {
      res.status(500).json({
        error: "Une erreur est survenue lors de la suppression du film.",
      });
    } else if (nbMoviesRemoved === 0) {
      res.status(404).json({ error: "Aucun film trouvé avec cet ID." });
    } else {
      res.status(200).json({ message: "Le film a été supprimé avec succès." });
    }
  });
});
