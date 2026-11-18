# Inventaire de compatibilité

Revue documentaire du 16 novembre 2026. Versions candidates, sans verrou de déploiement. La disponibilité des artefacts, la distribution effectivement embarquée et les tests croisés restent à vérifier.

| Bloc | Candidat et compatibilité documentaire | Qualification minimale avant pin |
|---|---|---|
| Spark SQL, Structured Streaming, MLlib | Spark 4.0.x, JDK 17 ou 21, Scala 2.13, Python ≥3.9. Choix de travail JDK 21 / Python 3.11. Le JDK 25 hôte est hors de la liste documentée de Spark 4.0.1. [Apache Spark](https://spark.apache.org/docs/4.0.1/) | Compter un DataFrame PySpark et exécuter une petite tâche MLlib sur l'image cible. |
| Delta | Delta 4.0.1 avec Spark 4.0.x, artefact Scala 2.13. [Delta Lake](https://docs.delta.io/releases/) | Écrire, relire et rejouer un MERGE fixture sur volume local. |
| Kafka | Kafka 4.1.x KRaft, JDK 21 pris en charge. Adaptateur Spark aligné sur la version Spark et le suffixe Scala. [Kafka](https://kafka.apache.org/41/operations/java-version/) | Tester accès broker et lecture Spark avec offsets conservés. |
| PostgreSQL & Debezium | PostgreSQL 16, pgoutput, Connect Kafka 4.1.x, Debezium 3.8.x candidat, JDK 21 satisfait le minimum connecteur 17. [Debezium](https://debezium.io/releases/) | Qualifier l'artefact Connect exact, vérifier snapshot et insert/update/delete, reconnexion et rôle CDC minimal. |
| Airflow | Airflow 3.4.x, Python 3.11 avec contraintes de la version et PostgreSQL 16. Image communautaire Debian Bookworm documentée. [Airflow](https://airflow.apache.org/docs/apache-airflow/stable/installation/prerequisites.html) | Démarrer métadonnées et exécuter une tâche isolée. |
| Superset | 6.1.x / Python 3.11, prévoir driver PostgreSQL et image BI séparée. [Superset](https://superset.apache.org/admin-docs/6.1.0/installation/pypi/) | Démarrer instance, vérifier rôle et mart en lecture. |
| MLflow | Serveur MLflow 3.x et client Python 3.11, tester la sérialisation du flavor Spark et les dépendances Scala si utilisées. [Serveur](https://mlflow.org/docs/latest/genai/getting-started/connect-environment/) · [Flavor Spark](https://mlflow.org/docs/latest/api_reference/python_api/mlflow.spark.html) | Journaliser, relire puis charger un modèle MLlib approuvé. |
| Great Expectations | GX Core 1.x, Python 3.11 dans la plage publiée. [GX](https://docs.greatexpectations.io/docs/core/set_up_a_gx_environment/install_gx/) | Tester une validation Spark valide et une ligne mise en quarantaine. |
| OS conteneurs | Linux amd64 sous Docker Desktop/WSL2, Debian Bookworm candidat pour bases propres. Les images fournisseurs gardent leur base supportée. | Vérifier architecture, `/etc/os-release`, version du runtime et volume Linux par image. |

Avant activation, choisir patchs supportés, vérifier le registre et l'architecture, tester les couples, scanner les images, verrouiller par digest et conserver les contraintes Python et la SBOM. Les versions Windows relevées se trouvent dans la preuve d'inventaire. Les tests de cette matrice sont NON TESTÉS.

## Qualification core au STEP 03

PostgreSQL 16.16 (Debian 12), Apache Kafka 4.1.2 (Alpine 3.23) et Debezium 3.4.3.Final (Fedora 41) ont été exercés ensemble en Linux amd64. Les digests sont fixés dans `config/.env.example` et `infra/compose/core.yml`. L'inventaire (archive de qualification locale) et le smoke (archive de qualification locale) documentent le sous-ensemble testé. La qualification CDC ne couvre pas les autres composants ni un scan complet des images.

## Qualification batch STEP 04

Spark 4.0.0, Scala 2.13.16, Java 17.0.16, Python 3.13.5 et Delta 4.0.1 ont été exercés ensemble sur Linux amd64. L'image est fixée par digest et les deux JAR Maven par SHA-256. La ligne Spark/Delta de la matrice est qualifiée pour le batch local. Streaming Kafka et MLlib restent à qualifier. Preuves (archive de qualification locale)
