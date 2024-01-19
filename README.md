# Atelier2_BigData_API_Kafka

## Explication rapide du projet

L'api utilise node js avec express pour récupérer les données de trafic d'une date donnée.<br>
Voici un exemple de l'utilisation de l'api : http://localhost:3000/api/trafic/2024-01-17T14:50:00+01:00

Ce projet utilise des données de trafic à une date et heure donnée pour calculer un indice moyen de congestion routière et pour calculer la vitesse moyenne des véhicule par tronçon. Les résultats sont ensuite diffusés via Apache Kafka.

## Installer les dépendances

```bash
npm install
```

## Démarrer l'API

```bash
npm run api
```

## Démarrer les producteurs

```bash
# taux de congestion
npm run pc

# vitesse moyenne
npm run pv
```

## Démarrer les consommateurs

```bash
# taux de congestion
npm run cc

# vitesse moyenne
npm run cv
```
