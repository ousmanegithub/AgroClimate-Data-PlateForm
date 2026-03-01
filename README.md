# AgriClimate Data Platform

## Vue d'ensemble

Bienvenue dans ce projet pratique de conception d’une **Data Platform moderne** appliquée à l’analyse agricole et climatique du Sénégal.

Ce projet met en œuvre une **architecture Medallion** (Bronze → Silver → Gold) afin d’intégrer, transformer et valoriser des données multi-sources à des fins décisionnelles.

Le projet est structuré en **trois grandes parties** complémentaires :

1. **Partie 1 : Data Lake avec Spark**  
   Construction d’un pipeline Data Engineering moderne :
- **PostgreSQL** : Base de données relationnelle (données territoriales et socio-économiques)
- **API Flask** (déployée sur PythonAnywhere) : Source de données climatiques
- **MinIO** : Stockage objet compatible S3 (Data Lake)
- **PySpark** : Traitement distribué
- **Marimo** : Notebook interactif moderne
- **Format Parquet** : Optimisation analytique

2. **Partie 2 : Architecture Medallion**  
   Mise en place d’un Data Lake structuré:
   **Bronze** : Données brutes

   **Silver** : Données nettoyées et enrichies

   **Gold** : Indicateurs décisionnels agrégés

3. **Partie 3 : Exploitation décisionnelle**  
   Création d’indicateurs stratégiques:
   Vulnérabilité climatique

   Efficacité des subventions agricoles

   Productivité ajustée au climat

   Sécurité alimentaire régionale

## Architecture

┌─────────────────────────────────────────────────────────────┐
│                     Docker Compose                           │
│                                                              │
│  ┌──────────────┐    ┌──────────┐    ┌────────────┐        │
│  │  PostgreSQL  │    │  MinIO   │    │  Marimo    │        │
│  │   (OLTP)     │────▶│ (S3/DL)  │◀───│(Notebook)  │        │
│  │   :5432      │    │  :9000   │    │  :8080     │        │
│  └──────────────┘    └──────────┘    └────────────┘        │
│         ▲                                    │              │
│         │                                    │              │
│     API Flask (Climate Data)                 │              │
│                                               │              │
│         Pipeline: Bronze → Silver → Gold                     │
└─────────────────────────────────────────────────────────────┘



## Objectif Métier

Répondre aux problématiques stratégiques suivantes :

- Quel est l’**impact du climat** sur la production agricole ?
- Les **subventions** améliorent-elles réellement le rendement ?
- Quelles régions sont **vulnérables** aux projections climatiques 2050 ?
- Comment **prioriser** les investissements en infrastructures rurales ?

Croisement de données :

- Données agricoles historiques (2003–2012)
- Données territoriales
- Données d’infrastructures
- Subventions agricoles
- Données climatiques historiques et prospectives

## Dataset

### 1. Données agricoles

Colonnes principales :

- région
- culture
- annee
- superficie_ha
- rendement_kg_ha
- production_tonnes
- pluviometrie_moyenne
- population_rurale
- nb_forages
- nb_centres_stockage
- km_routes_rurales
- montant_total_fcfa (subventions)
- nb_beneficiaires
- etc.

Période : **2003–2012**

### 2. Données PostgreSQL

- Régions administratives (14 régions du Sénégal)
- Infrastructures rurales
- Subventions agricoles

### 3. API Climatique (Flask sur PythonAnywhere)

Endpoint principal :


Variables fournies :

- `annual_avg_temp` : Température moyenne annuelle (°C)
- `annual_avg_precip` : Précipitations annuelles (mm/an)
- `annual_avg_humidity` : Humidité relative moyenne (%)
- `annual_avg_solar_rad` : Radiation solaire (MJ/m²/jour)
- `annual_avg_wind_speed` : Vitesse du vent (m/s)
- `projected_temp_2050` : Température projetée 2050 (°C)
- `projected_precip_change_2050` : Changement précipitations 2050 (%)
- `sea_level_rise_2100` : Hausse niveau de la mer 2100 (m) – régions côtières

## Architecture Medallion

### Couche Bronze

- Stockage brut des données
- Données agricoles importées
- Tables PostgreSQL exportées
- Données API au format JSON
- Pas de jointure ni de nettoyage
- Format : **Parquet**
- Bucket : `bronze`

### Couche Silver

- Structuration et enrichissement
- Harmonisation noms de régions
- Cast des types numériques
- Jointures multiples :
  - Agriculture ↔ Régions
  - Agriculture ↔ Infrastructures
  - Agriculture ↔ Subventions
  - Agriculture ↔ Données climatiques
- Création indicateurs intermédiaires
- Bucket : `silver`

### Couche Gold

Indicateurs décisionnels prêts à l’analyse :

- **Indice de vulnérabilité climatique**  
  (basé sur hausse température + baisse précipitations + pluviométrie actuelle)

- **Indice d’efficacité des subventions**  
rendement_kg_ha / valeur_subvention_par_beneficiaire


- **Productivité ajustée climat**  
Production pondérée par variables climatiques

- **Score d’infrastructure rurale**  
Combinaison : nb_forages + centres_stockage + km_routes_rurales

- **Indicateur de sécurité alimentaire**  
Production / population_rurale ajustée par stress climatique

Bucket : `gold`

## Logique Analytique

Production Agricole
↓
Influencée par
↓
Climat + Infrastructures + Subventions


Permet :

- Analyse corrélative
- Analyse prospective 2050
- Analyse d’impact politique

## Installation et Démarrage

### Structure du projet

agriclimate-data-platform/
├── docker-compose.yml
├── qata/
│   └── agriculture
├── notebooks/
│   └── agro_climate.py           # Notebook Marimo
├── postgres/
│   └── init.sql           
└── README.md


### Lancement

```bash
# Démarrer tous les services
docker-compose up -d

# Vérifier l'état
docker-compose ps

# Voir les logs en temps réel
docker-compose logs -f


Accès aux interfaces

Marimo Notebook : http://localhost:8080
MinIO Console : http://localhost:9001
Identifiants :
Login : minioadmin
Password : minioadmin123


# Arrêter tout
docker-compose down

# Arrêter + supprimer volumes
docker-compose down -v

# Rebuild sans cache
docker-compose build --no-cache

# Redémarrer un service spécifique
docker-compose restart minio