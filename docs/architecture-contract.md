# Architecture et traçabilité

Flux cible Lambda. M5 → Spark batch → Bronze. Transferts opérationnels PostgreSQL → Debezium/Connect → Kafka → Spark Structured Streaming → Bronze. Great Expectations contrôle le passage en Silver. Gold réconcilie par clé et version. Spark SQL produit les marts, Spark MLlib les prévisions journalisées dans MLflow. Airflow orchestre, Prometheus/Grafana observent. FastAPI sert les décisions à Next.js. Superset lit des marts autorisés. Les équipes utilisent le logo approuvé du pack. La recette suivante décrit les preuves à produire quand chaque composant sera livré.

| Exigences | Cible | Test et preuve attendus |
|---|---|---|
| F01, F03 / R01 | Batch Spark, Delta Bronze et catalogue | Hash d'import, provenance et liste des sources |
| F02 / R01 | PostgreSQL, Debezium, Kafka, Spark streaming | Offset, opération et lignes à chaque couche |
| F04, F05 / R02, R03 | Great Expectations, Silver et Gold Delta | Quarantaine d'une ligne invalide, agrégats invariants après rejeu |
| F06 / R05 | MLlib, fenêtre gelée et MLflow | MAE/WAPE, baseline et identifiant du run |
| F07–F09 / R06, R07 | Service de décision et FastAPI | Calcul explicable, contrôle de concurrence et audit de la décision |
| F10, F13 / R04 | Spark SQL, Superset et React/Next.js | Marts, vérification KPI et export CSV |
| F11 / R08 | Airflow | Exécution d'un DAG et reprise |
| F12 / R08 | Prometheus et Grafana | Santé et alerte vérifiées |
| F14 / R09 | OIDC et RBAC/ABAC API, SQL et BI | Refus croisés à deux tenants et deux sites |
| Socle STEP 01 | Git, Compose, preflight, scanner | Rapport et preuves exécutées (archive de qualification locale) |
| Données STEP 02, F01–F05 | Adaptateur local M5, événements v1 et générateur distinct | Rapport et preuves (archive de qualification locale) |
| Entrée CDC STEP 03, F02 / R01 | Publication PostgreSQL et enveloppes brutes Debezium sur Kafka | Rapport et preuves (archive de qualification locale) |
| Batch Bronze STEP 04, F01/F03 et R01 | M5 local vers Delta append-only avec registre et quarantaine | Rapport et preuves (archive de qualification locale) |
| Streaming Bronze STEP 05, F02/F03 et R01/R02 | Spark Structured Streaming lit le CDC natif et conserve reçus et mutations dédupliquées dans Delta | Rapport et preuves (archive de qualification locale) |
| Silver/Gold STEP 06, F04/F05 et R02/R03 | Snapshots Bronze épinglés, qualité GX et réconciliation explicite des fixtures | Rapport et preuves (archive de qualification locale) |
| Prévision STEP 07, F06 / R05 | Variables temporelles et trois références de prévision Spark sur fixtures isolées | [Dictionnaire](ml/feature_dictionary.md) · Rapport et preuves (archive de qualification locale) |

## Risques ouverts

| Risque | Priorité | Condition de fermeture |
|---|---|---|
| Secrets et dépendances d'exécution | P0 | Scanner l'ensemble des artefacts/images, verrouiller les versions, vérifier droits et rotation |
| Accès inter-tenant/API/SQL/BI | P0 | Refus effectifs avec identités, rôles et sites distincts |
| Rejeu CDC, correction et double approbation | P0 | Tests crash/restart, invariance Gold et transition atomique |
| RAM, WAL et espace disque | P1 | Mesures par profil, limites de rétention et alertes |
| Confusion des sources et rupture supposée dans M5 | P1 | Tests de provenance et de simulation, documentation des biais |
| Exposition admin et privilèges CI | P0 | Validation des routes réseau et des identités CI après mise en place |

Les ADR fixent les choix au niveau de la conception. Les contraintes de données sont dans [le contrat canonique](../contracts/data.md). Le test de Compose vérifie le modèle figé du STEP 01. Les scénarios F/R et les mesures d'exécution restent à effectuer aux étapes concernées.
