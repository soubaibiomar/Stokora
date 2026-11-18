# STOKORA

STOKORA transforme des ventes synthétiques en prévisions quotidiennes par tenant, magasin et produit. Le projet Docker Compose **stokora-local** exécute quatre services dédiés sur un réseau privé. Le profil cible la démonstration et le développement académique sur une seule machine.

~~~mermaid
flowchart LR
  subgraph S[stokora-local]
    P[stokora-postgres] --> C[stokora-connect]
    C --> K[stokora-kafka]
    K --> J[stokora-spark]
    J --> D[Delta Bronze, Silver, Gold]
    D --> F[Prévisions à sept jours]
  end
~~~

## Démarrer

Docker Desktop en mode Linux amd64, Docker Compose v2 et PowerShell 7. Prévoir au moins 8 Gio de mémoire pour le moteur Docker et un noyau Linux compatible avec Landlock réseau (ABI 4+). Le premier build récupère des dépendances publiques épinglées. Les traitements utilisent le CPU. Python, Java, Spark et PostgreSQL sont fournis par Docker.

~~~powershell
./scripts/stokora.ps1 build
./scripts/stokora.ps1 up
./scripts/stokora.ps1 status
./scripts/stokora.ps1 test
./scripts/stokora.ps1 generate
./scripts/stokora.ps1 pipeline
./scripts/stokora.ps1 forecast
~~~

Le fichier canonique est infra/compose/local.yml. Le générateur applique la graine 42 une seule fois. Le pipeline effectue la lecture CDC, publie Bronze/Silver/Gold et vérifie la correction du calendrier. La prévision publie un manifeste et ses métriques dans le volume dédié. La commande recover vérifie une interruption après le commit Delta puis un rejeu depuis le début sur des fichiers temporaires.

## Exploitation

~~~powershell
./scripts/stokora.ps1 logs
./scripts/stokora.ps1 stop
./scripts/stokora.ps1 restart
./scripts/verify_local_compose.ps1
~~~

Un second lancement réutilise les services de stokora-local. Les tests et les traitements exécutent des commandes dans Spark. L’arrêt préserve les données. Les volumes nommés stockent séparément PostgreSQL, la métadonnée et les offsets Kafka, les identités TLS, les checkpoints, les tables Delta et la prévision. Le provisionnement de la nouvelle instance échoue si son format est partiel ou inconnu. La destruction d’un volume retire les données correspondantes.

| Service | Ressources maximales |
|---|---:|
| PostgreSQL | 1 CPU et 768 Mio |
| Kafka | 1 CPU et 1 Gio |
| Debezium Connect | 1 CPU et 1 Gio |
| Spark local | 2 CPU et 3 328 Mio |

Kafka utilise SASL_SSL et des ACL par topic et groupe. PostgreSQL vérifie SCRAM via TLS. L’API de gestion Connect exige un certificat client. Le réseau privé n’expose aucun port sur l’hôte. Les certificats locaux expirent après 89 jours. Le superviseur de PostgreSQL initialise les identités des autres services au premier lancement. La séparation des processus et des volumes de données réduit le périmètre des accès courants. Le superviseur de provisionnement reste une frontière de confiance.

## Référence STEP 07

Sur l’entrée Gold figée, la référence couvre 783 origines, 16 443 prévisions et 800 observations évaluées par méthode.

| Méthode | MAE | WAPE |
|---|---:|---:|
| Dernière observation | 0,8613 | 21,23 % |
| Même jour de la semaine précédente | 0,0300 | 0,74 % |
| Moyenne mobile | 0,7381 | 18,20 % |

**SEC-01 reste PARTIAL et STEP 08 reste BLOCKED.** Les vulnérabilités de l’image empêchent sa promotion. Le droit d’utiliser le jeu M5 réel demeure à confirmer. Le parcours exploite les fixtures synthétiques livrées avec le code. [Guide Docker](reports/infrastructure/DOCKER_LIFECYCLE_GUIDE.md)

## Developer handover

Start with the [developer guide](docs/HANDOVER.md), [setup checklist](docs/HANDOVER_CHECKLIST.md), and [agent rules](AGENTS.md).
