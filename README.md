# ✈️ Analyse des Retards de Vols Aériens avec Hadoop

Projet Big Data réalisé sur **Google Cloud Platform (Dataproc)** dans le cadre du Master à l'Université Paris 8 — 2025/2026.

---

## 📊 Dataset

| Propriété | Détail |
|---|---|
| Source | [Kaggle - Airlines Dataset](https://www.kaggle.com/datasets/jimschacko/airlines-dataset-to-predict-a-delay) |
| Format | CSV |
| Taille | ~18.51 MB |
| Nombre de vols | 539 383 |
| Colonnes | id, Airline, Flight, AirportFrom, AirportTo, DayOfWeek, Time, Length, Delay |

---

## 🛠️ Technologies utilisées

- **Hadoop 3.3.6** — HDFS, MapReduce, YARN
- **Hive 3.1.3** — Requêtes analytiques HQL
- **Python 3** — Hadoop Streaming
- **Google Cloud Dataproc** — Cluster managé

---

## 🗂️ Structure du projet

```
hadoop-airlines-delay/
├── job1_delays_by_airline.py     # Retards par compagnie aérienne
├── job2_flights_by_airport.py    # Vols par aéroport de départ
├── job3_delays_by_day.py         # Retards par jour de la semaine
├── job4_avg_duration_by_route.py # Durée moyenne par route
└── airlines_analysis.hql         # 15 requêtes Hive analytiques
```

---

## ⚙️ Jobs MapReduce

| Job | Objectif | Output |
|---|---|---|
| Job 1 | Nombre de retards par compagnie | `Airline \t Nb_retards` |
| Job 2 | Nombre de vols par aéroport de départ | `Airport \t Nb_vols` |
| Job 3 | Taux de retard par jour de la semaine | `Jour \t Total \t Retards \t %` |
| Job 4 | Durée moyenne de vol par route | `Route \t Durée_moy \t Nb_vols` |

### Lancer un job

```bash
# Supprimer le répertoire de sortie s'il existe
hdfs dfs -rm -r /user/hadoop/airlines/output/job1_delays

# Lancer le job
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files job1_delays_by_airline.py \
  -mapper "python3 job1_delays_by_airline.py mapper" \
  -reducer "python3 job1_delays_by_airline.py reducer" \
  -input /user/hadoop/airlines/data/airlines.csv \
  -output /user/hadoop/airlines/output/job1_delays
```

---

## 🐝 Requêtes Hive

15 requêtes HQL développées dans `airlines_analysis.hql` :

1. Création de la table externe `flights`
2. Statistiques globales
3. Top 10 compagnies avec le plus de retards
4. Top 20 aéroports de départ les plus fréquentés
5. Top 20 aéroports d'arrivée les plus fréquents
6. Analyse des retards par jour de la semaine
7. Analyse des retards par heure de départ
8. Top 30 routes les plus fréquentées
9. Top 20 routes avec le plus de retards
10. Durée moyenne de vol par compagnie
11. Taux de retards par compagnie et jour
12. Taux de retards par vols courts et longs
13. Analyse par tranche horaire
14. Top 20 aéroports les plus problématiques
15. Comparaison des performances par compagnie

---

## 📈 Résultats clés

- 🌍 **Taux de retard global : 44,54%** (240 264 vols sur 539 383)
- ✈️ **Compagnie la plus retardée : WN (Southwest Airlines)** — 65 657 retards
- 🏢 **Aéroport le plus problématique : MDW (Chicago Midway)** — 73,5% de retards
- 📅 **Jour le plus risqué : Mercredi** — 47,1% de retards
- ⏱️ **Durée moyenne de vol : 132 minutes**

---

## ☁️ Infrastructure GCP

```
Plateforme   : Google Cloud Dataproc
Région       : europe-west1
Master node  : n4-standard-2 (2 vCPUs, 7.5 GB RAM)
Worker nodes : n4-standard-2 x2
Hadoop       : 3.3.6
Hive         : 3.1.3
```

---

## 👩‍💻 Auteure

**Litissia LARBI** — Master Université Paris 8, 2025/2026
