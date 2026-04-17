# Agriclimate Data Platform

## Présentation générale du projet

Le projet **Agriclimate Data Platform** consiste à concevoir une plateforme moderne de gestion et de traitement de données appliquée au domaine **agricole et climatique au Sénégal**.

L’objectif principal est de mettre en place une infrastructure capable de :

- collecter plusieurs sources de données hétérogènes ;
- traiter des données historiques en mode batch ;
- intégrer des flux de données en temps réel ;
- détecter automatiquement des anomalies ;
- stocker les données selon une architecture industrielle ;
- exposer des indicateurs analytiques exploitables pour les décideurs.

Ce projet répond à un besoin métier réel :

> améliorer le pilotage des politiques agricoles grâce à une meilleure exploitation des données climatiques et agronomiques.

---

# Objectifs du projet

Les objectifs techniques et métiers du projet sont les suivants :

## Objectifs techniques

- Construire une architecture de données moderne
- Mettre en place une ingestion multi-sources
- Implémenter un pipeline batch
- Implémenter un pipeline streaming
- Détecter des anomalies en temps réel
- Structurer les données selon une architecture Medallion
- Exposer des données prêtes à l’analyse

## Objectifs métiers

- Surveiller l’état des parcelles agricoles
- Mesurer l’impact du climat sur les rendements
- Identifier les zones à risque
- Suivre l’efficacité des subventions
- Aider à la prise de décision agricole

---

# Architecture globale de la plateforme

La plateforme repose sur une architecture hybride combinant :

- traitement batch
- traitement streaming
- stockage cloud
- orchestration
- analyse


Sources de données
│
├── Données agricoles CSV
├── Base PostgreSQL
├── API climatique
├── API capteurs temps réel
└── Application événements terrain
        │
        ▼
      Kafka
        │
        ▼
      MinIO
        │
 ┌──────┼──────┐
 ▼      ▼      ▼
Bronze Silver Gold
        │
        ▼
     Airflow
        │
        ▼
 Indicateurs métier

# Architecture fonctionnelle et technique du projet

Le projet **Agriclimate Data Platform** repose sur une architecture de données complète permettant de combiner à la fois :

- l’ingestion de données historiques,
- le traitement analytique,
- le streaming en temps réel,
- et la production d’indicateurs métiers.

L’ensemble de la solution a été structuré selon deux grandes composantes complémentaires :

1. **Une plateforme batch** pour consolider les données historiques.
2. **Une plateforme streaming** pour la supervision temps réel.

Cette séparation permet de répondre simultanément aux besoins d’analyse stratégique et aux besoins de surveillance opérationnelle.

---

# Première partie : Data Platform Batch

La première partie du projet concerne la gestion et l’exploitation des **données historiques agricoles et climatiques**.

L’objectif principal est de construire une plateforme capable de centraliser plusieurs sources de données afin de produire une base analytique fiable.

---

## Sources batch utilisées

Trois sources principales ont été utilisées dans cette première phase.

---

## 1. Données agricoles CSV

La première source est constituée de plusieurs fichiers CSV contenant les statistiques agricoles nationales.

Ces fichiers regroupent notamment :

- les cultures produites,
- les superficies emblavées,
- les rendements agricoles,
- les volumes de production,
- les régions administratives,
- les années de production.

### Variables principales

| Variable | Description |
|---------|-------------|
| `region` | Région administrative |
| `culture` | Type de culture |
| `annee` | Année agricole |
| `superficie_ha` | Superficie cultivée en hectares |
| `rendement_kg_ha` | Rendement moyen par hectare |
| `production_tonnes` | Production totale en tonnes |

### Justification métier

Ces données permettent :

- d’analyser l’évolution de la production agricole,
- de comparer les régions,
- d’identifier les cultures les plus performantes,
- de mesurer l’impact des facteurs climatiques sur la production.

---

## 2. Base PostgreSQL

La deuxième source est une base de données relationnelle PostgreSQL contenant les données structurelles.

Cette base stocke :

- les caractéristiques des régions,
- les infrastructures agricoles,
- les données de subventions,
- les populations rurales,
- les indicateurs territoriaux.

### Tables principales

#### Table `regions`

| Variable | Description |
|---------|-------------|
| `id` | Identifiant région |
| `nom_region` | Nom de la région |
| `code_region` | Code administratif |
| `zone_agro_ecologique` | Zone agroécologique |
| `pluviometrie_moyenne` | Pluviométrie moyenne |
| `population_rurale` | Population rurale |

#### Table `infrastructures`

| Variable | Description |
|---------|-------------|
| `region_id` | Référence région |
| `annee` | Année |
| `nb_forages` | Nombre de forages |
| `nb_centres_stockage` | Centres de stockage |
| `km_routes_rurales` | Longueur des routes rurales |

#### Table `subventions_agricoles`

| Variable | Description |
|---------|-------------|
| `region_id` | Référence région |
| `annee` | Année |
| `montant_total_fcfa` | Montant total |
| `nb_beneficiaires` | Nombre de bénéficiaires |
| `type_subvention` | Type d’aide |

### Justification métier

Ces données permettent :

- de contextualiser la production agricole,
- d’évaluer l’impact des investissements publics,
- de mesurer l’efficacité des politiques agricoles.

---

## 3. API Climate

La troisième source est une API REST exposant des indicateurs climatiques régionaux.

Elle fournit :

- température moyenne,
- humidité,
- précipitations,
- rayonnement solaire,
- vitesse du vent,
- projections climatiques futures.

### Variables climatiques

| Variable | Description |
|---------|-------------|
| `annual_avg_temp` | Température annuelle |
| `annual_avg_precip` | Précipitations annuelles |
| `annual_avg_humidity` | Humidité moyenne |
| `annual_avg_solar_rad` | Rayonnement solaire |
| `annual_avg_wind_speed` | Vitesse du vent |
| `projected_temp_2050` | Température projetée |
| `projected_precip_change_2050` | Variation future pluie |
| `sea_level_rise_2100` | Hausse niveau marin |

### Justification métier

Cette source apporte une dimension stratégique permettant :

- d’évaluer les risques climatiques,
- d’anticiper les pertes,
- d’améliorer la résilience agricole.

---

# Architecture Medallion

Le projet suit une architecture moderne **Bronze → Silver → Gold**.

---

## Bronze Layer

Le niveau Bronze stocke les données brutes sans transformation métier majeure.

### Contenu

- CSV agricoles originaux
- données PostgreSQL extraites
- données JSON de l’API

### Objectifs

- conserver la donnée source,
- assurer la traçabilité,
- garantir la reproductibilité.

---

## Silver Layer

Le niveau Silver contient les données nettoyées et enrichies.

### Transformations appliquées

- correction des noms de régions,
- normalisation des types,
- suppression des incohérences,
- jointures inter-sources,
- enrichissement métier.

### Indicateurs intermédiaires

#### Valeur de subvention par bénéficiaire


montant_total_fcfa / nb_beneficiaires

### Production par habitant rural

Le calcul de la **production par habitant rural** permet d’estimer la capacité de production agricole d’une région par rapport à sa population active rurale.

**Formule utilisée :**


production_par_habitant_rural = production_tonnes / population_rurale


Cette métrique permet de mesurer :

- la pression démographique sur la production ;
- la disponibilité agricole locale ;
- le niveau potentiel d’autosuffisance alimentaire ;
- les écarts territoriaux de productivité.

### Objectifs de la couche Silver

La couche Silver vise à :

- améliorer la qualité des données ;
- homogénéiser les structures entre sources ;
- corriger les incohérences ;
- préparer les données pour les analyses avancées ;
- centraliser les enrichissements métier.

### Gold Layer

Le niveau Gold contient les indicateurs analytiques directement exploitables par les décideurs.

#### Indicateurs produits

**Indice de risque climatique**

Cet indicateur mesure la vulnérabilité climatique globale d’une région à partir de :

- du déficit pluviométrique ;
- de l’augmentation des températures ;
- de la variation climatique future projetée.

Il permet d’identifier les zones agricoles les plus exposées.

**Rendement ajusté au climat**

Le rendement agricole est ajusté en fonction de plusieurs variables environnementales :

- température moyenne ;
- humidité ;
- précipitations ;
- stress hydrique.

Ce calcul permet d’obtenir une estimation plus réaliste de la performance agricole.

**Score de vulnérabilité économique**

Cet indicateur mesure :

- la dépendance climatique ;
- la productivité locale ;
- la fragilité économique régionale.

Il permet de détecter les régions nécessitant une intervention prioritaire.

#### Objectifs de la couche Gold

Le niveau Gold permet de :

- faciliter l’analyse décisionnelle ;
- alimenter les outils de BI ;
- fournir des KPI métier ;
- soutenir la prise de décision publique.

---

## Deuxième partie : Pipeline Temps Réel

La seconde partie du projet ajoute une surveillance continue des exploitations agricoles.

### Source temps réel 1 : API Capteurs agricoles

Une application Flask simule un réseau de capteurs connectés déployés sur les parcelles.

#### Variables atmosphériques

| Variable | Description |
|----------|-------------|
| temperature_air | Température de l’air |
| humidite_air | Humidité de l’air |
| vitesse_vent | Vitesse du vent |
| rayonnement_solaire | Intensité de l’ensoleillement |

Ces variables permettent de surveiller le microclimat local.

#### Variables hydriques

| Variable | Description |
|----------|-------------|
| pluviometrie | Précipitations |
| humidite_sol | Humidité du sol |
| evapotranspiration | Perte d’eau par évaporation |

Ces mesures permettent d’évaluer le stress hydrique.

#### Variables du sol

| Variable | Description |
|----------|-------------|
| ph_sol | Acidité du sol |
| azote | Teneur en azote |
| phosphore | Teneur en phosphore |
| potassium | Teneur en potassium |
| carbone_organique | Matière organique |

Ces données décrivent la fertilité des sols.

#### Variables de stress

| Variable | Description |
|----------|-------------|
| indice_stress_hydrique | Niveau de stress hydrique |
| temperature_sol | Température du sol |

Ces indicateurs servent à détecter les situations critiques.

### Source temps réel 2 : Événements terrain

Une deuxième application génère les événements opérationnels observés sur les parcelles.

#### Exemples d’événements

- irrigation ;
- fertilisation ;
- maladie ;
- traitement phytosanitaire ;
- intervention technique.

Ces données complètent les mesures capteurs pour enrichir l’analyse.

### Rôle de Kafka

Kafka agit comme bus de streaming central de la plateforme.

#### Topics utilisés

**Topics bruts**

- `sensor.raw`
- `field.raw`

Ces topics reçoivent les données sans transformation.

**Topics anomalies**

- `sensor.anomalies`
- `field.anomalies`

Ces topics contiennent uniquement les événements jugés anormaux.

### Rôle du Producer

Le Producer :

- interroge l’API Flask ;
- récupère les nouvelles mesures ;
- transforme les messages ;
- sérialise les données ;
- envoie les messages dans Kafka.

Le Producer constitue le point d’entrée du pipeline temps réel.

### Rôle du Consumer

Le Consumer :

- lit les messages Kafka ;
- applique les règles métier ;
- détecte les anomalies ;
- redirige les événements critiques.

#### Exemples d’anomalies détectées

- température excessive ;
- humidité trop faible ;
- pH critique ;
- stress hydrique élevé.

Les anomalies sont :

- isolées ;
- historisées ;
- envoyées sur un topic dédié.

### Rôle de Airflow

Airflow orchestre automatiquement :

- les batchs quotidiens ;
- les contrôles qualité ;
- la consolidation des données ;
- la mise à jour du Gold Layer.

Airflow garantit la fiabilité globale de la plateforme.

---

## Technologies utilisées

### Infrastructure

- Docker
- Docker Compose

### Traitement

- Python
- PySpark

### Streaming

- Apache Kafka
- Kafka UI

### Stockage

- PostgreSQL
- MinIO

### Orchestration

- Apache Airflow

### Développement

- Marimo
- Flask

---

## Pertinence métier

Le projet répond à plusieurs enjeux stratégiques.

### Agriculture de précision

La plateforme permet :

- une surveillance continue ;
- une réaction rapide ;
- une optimisation des intrants ;
- une meilleure productivité.

### Adaptation climatique

Elle permet :

- l’anticipation des sécheresses ;
- la gestion du risque ;
- l’amélioration de la planification agricole.

### Gouvernance agricole

Elle facilite :

- le suivi des aides ;
- le pilotage régional ;
- la décision publique.

---

## Axes d'amélioration

Plusieurs évolutions peuvent renforcer la plateforme.

### Machine Learning

Ajout possible de :

- prédiction des rendements ;
- forecasting climatique ;
- classification avancée des risques.

### Monitoring

Intégration de :

- Prometheus
- Grafana

### Data Quality

Ajout de :

- Great Expectations ;
- alertes automatiques ;
- scoring qualité.

### Visualisation

Développement de dashboards via :

- Power BI
- Streamlit
- interfaces métier web.

---

## Conclusion

Le projet **Agriclimate Data Platform** illustre la mise en place d’une plateforme de données moderne appliquée au domaine agricole.

Il combine :

- ingestion multi-sources ;
- traitement batch ;
- streaming temps réel ;
- détection d’anomalies ;
- stockage cloud ;
- exposition analytique.

### Apport Data Engineering

Le projet démontre la maîtrise :

- des pipelines de données ;
- de l’orchestration ;
- du streaming ;
- de l’architecture Medallion ;
- de l’intégration cloud.

### Apport métier

Sur le plan métier, il fournit un socle robuste pour :

- renforcer la résilience agricole ;
- améliorer la surveillance des cultures ;
- optimiser les ressources ;
- accompagner la transition climatique du Sénégal.