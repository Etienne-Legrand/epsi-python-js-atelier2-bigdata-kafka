# Atelier2_BigData_API_Kafka

## Explication rapide du projet

L'api utilise Node.js avec Express.js pour récupérer les données de trafic d'une date donnée.<br>
Voici un exemple de l'utilisation de l'api : http://localhost:3000/api/trafic/2024-01-17T14:50:00+01:00

Ce projet utilise des données de trafic à une date et heure donnée pour calculer un indice moyen de congestion routière et pour calculer la vitesse moyenne des véhicules par tronçon. Les résultats sont ensuite diffusés via Apache Kafka.

## Prérequis

Avoir :
- installé Node.js, python et Java
- un serveur Kafka de démarré sur l'URL localhost:9092.
- (optionnel) un serveur flink de démarré 

## Installer les dépendances

```bash
npm install
```

## Démarrer l'API

```bash
npm run api
# OU
node api/index.js
```

## Démarrer les producteurs

```bash
# taux de congestion
npm run pc
  # OU
node producteur/producteur_congestion.js

# vitesse moyenne
npm run pv
  # OU
node producteur/producteur_vitesse.js
```

## Démarrer les consommateurs

```bash
# taux de congestion
npm run cc
  # OU
node consommateur/consommateur_congestion.js

# vitesse moyenne
npm run cv
  # OU
node consommateur/consommateur_vitesse.js
```
