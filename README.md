# STOKORA

STOKORA est un projet de prévision de la demande en magasin. Il rapproche des ventes importées en batch et des changements capturés dans PostgreSQL, puis construit une série quotidienne par tenant, magasin et produit. Les données synthétiques permettent de vérifier le rejeu, les corrections tardives et les prévisions sans utiliser les ventes réelles de M5.

## Parcours d’une vente

| Couche | Rôle |
|---|---|
| Collecte | Les imports CSV et les événements PostgreSQL capturés par Debezium et Kafka alimentent deux chemins distincts. |
| Bronze | Spark conserve les entrées, leur provenance et les rejets dans Delta Lake. Les événements Kafka gardent leur identité et leur position. |
| Silver et Gold | La réconciliation contrôle les clés, les conflits de calendrier et les droits du tenant. Gold agrège les quantités quotidiennes. |
| Prévision | Les variables sont calculées à la date d’origine. Trois références prédisent les sept jours suivants à partir de la dernière observation, de la semaine précédente ou d’une moyenne mobile. |

## Essai local sur données synthétiques

Depuis la racine du dépôt, avec Python et PowerShell, les commandes suivantes installent la dépendance de validation des contrats et exécutent les tests du générateur et de l’adaptateur. Docker et les fichiers M5 réels ne sont pas nécessaires pour cet essai.

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements-step02.txt
.\.venv\Scripts\python -m unittest discover -s tests/data -v
```

Pour générer un petit jeu de transactions reproductible, choisir un répertoire de sortie neuf.

```powershell
.\.venv\Scripts\python -m pipelines.generators.transactions --seed 42 --output data/local/sim-42
```

## Résultats vérifiés sur la fixture figée

La comparaison porte sur **783 origines**, **16 443 lignes de prévision** et **800 prévisions de test évaluées par méthode**. La MAE est exprimée en unités de vente. La WAPE est rapportée au volume observé.

| Méthode | MAE | WAPE |
|---|---:|---:|
| Dernière observation | 0,8613 | 21,23 % |
| Même jour de la semaine précédente | 0,0300 | 0,74 % |
| Moyenne mobile | 0,7381 | 18,20 % |

Ces mesures décrivent la fixture synthétique. Le benchmark complet dépend des snapshots Gold et des volumes de qualification de la station de référence. Les fichiers du dépôt suffisent pour exécuter l’essai Python ci-dessus.

## État et limites

STEP 07 dispose d’une référence synthétique vérifiée. **STEP 08 et le déploiement restent bloqués** par le contrôle des images de service. Les résultats de CDC, d’isolation et de prévision portent sur les scénarios testés. L’autorisation d’usage de M5 et la disponibilité historique réelle des observations restent ouvertes.

| Pour explorer | Point d’entrée |
|---|---|
| Contrats des événements | [Schéma JSON](contracts/events/v1.schema.json) |
| Modèle opérationnel et capture CDC | [Migrations PostgreSQL](infra/db/migrations/) · [Connecteur](infra/kafka/connectors/) |
| Imports, quarantaine et rejeu | [Batch](pipelines/batch/) · [Streaming](pipelines/streaming/) |
| Réconciliation et agrégats | [Silver](pipelines/silver/) · [Gold](pipelines/gold/) |
| Prévisions et hypothèses | [Dictionnaire des variables](docs/ml/feature_dictionary.md) · [Tests temporels](tests/ml/) |
| Configuration Docker | [Profils Compose](infra/compose/) |

Les identifiants, les volumes de travail et les jeux de ventes réels restent locaux. La licence du dépôt couvre le code publié. Les conditions de réutilisation des données tierces relèvent de leurs fournisseurs.
