# CDC vers Bronze Delta

STEP 05 lit uniquement le Kafka de démonstration `stokora-stream-fixture`, sur son réseau interne. Le conteneur Spark reste non-root, sans port hôte, sans Jupyter ou UI Spark, avec 2 vCPU et 3 GiB. Le compte Kafka de lecture accède seulement au topic brut et aux groupes autorisés. Il reçoit un JAAS en fichier monté. Le prototype utilise SASL/PLAIN sur le réseau privé et requiert TLS avant un déploiement hors du laboratoire.

Depuis la racine PowerShell, vérifier les JAR Maven et créer le scénario une fois :

```powershell
./scripts/fetch_streaming.ps1
python -m scripts.setup_stream_fixture
docker compose -f infra/compose/streaming.yml --profile streaming run --rm -T --name stokora-stream-spark spark --once
```

Les services initiaux de STEP 03 et leurs volumes sont distincts. Le setup injecte des ventes synthétiques dans PostgreSQL, puis redélivre un ancien insert et injecte trois messages invalides dans le Kafka de test. Ne pas réutiliser la cible de validation pour une ingestion opérationnelle. Pour arrêter le core de démonstration, utiliser Compose avec le même nom, les mêmes fichiers et les chemins secrets de ce projet. Conserver la table et les checkpoints. Voir les commandes exactes dans le rapport.

## Contrat et rejeu

Le volume `stokora-streaming-bound_stream_data` contient `cdc_bronze`, `checkpoint-main` et, avec `--replay`, un deuxième checkpoint persistant. L'ancien volume `stokora-streaming_stream_data` reste disponible en lecture pour inspection historique. Aucune table batch n'est montée. Le consumer limite les micro-lots à 1 000 offsets et garde `failOnDataLoss`. Un verrou Linux limite à un écrivain coopérant local par volume. Le flux est conservé sans watermark de suppression. La capacité de stockage et la rétention Kafka doivent être surveillées.

La table append-only contient deux catégories.

- `receipt` conserve clé et valeur Kafka en octets et leurs SHA-256 séparés. Une valeur Kafka nulle a un hash nul, distinct du SHA-256 d'une valeur vide. Son identifiant couvre le cluster, l'UUID et le nom du topic, la partition et l'offset. `disposition` vaut accepted, tombstone ou quarantine. Un tombstone à clé valide conserve le reçu. Les diagnostics de conflit incluent le reçu et la position, sans contenu.
- `event` conserve une mutation dédupliquée `r/c/u/d`, les champs de provenance validés et son `receipt_id` d'origine. Le classificateur vérifie l'origine Debezium, le LSN, les timestamps, les images avant/après, la clé composite, la devise, la date, la quantité et le marqueur synthétique.

Pour une mutation, la clé SHA-256 couvre l'époque issue de l'identité Kafka vérifiée, l'identité de la table source, la clé composite, l'opération, LSN, transaction, séquence, timestamp source et images canoniques. Une rediffusion à un nouvel offset conserve son propre reçu. Deux changements ayant exactement les mêmes coordonnées source et images partagent l'identité de mutation. Une recréation du checkpoint conserve la déduplication tant que les offsets restent disponibles.

Le sink compare la clé et la valeur octet par octet, y compris dans un même micro-lot. Tout conflit arrête le lot avant le MERGE et laisse le dernier snapshot lisible. Un rejeu identique n'envoie aucune écriture à Delta. Pour les nouveaux reçus, un `MERGE` insert-only publie reçus et mutations dans la même transaction. `DESCRIBE HISTORY` lie chaque écriture à la tentative et au batch. Les garanties testées concernent un écrivain local, un Kafka stable et un Delta disponible. La projection d'essai applique les opérations dans l'ordre source.

## Liaison Kafka et changement de source

L'AdminClient Kafka emploie la même configuration SASL que Spark et vérifie le cluster et l'UUID de topic au démarrage puis avant chaque publication, après lecture des lignes. Voir les API de [Kafka Admin](https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/admin/Admin.html) et de [TopicDescription](https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/admin/TopicDescription.html). L'identité initiale vient du broker authentifié sur le réseau configuré. La propriété `stokora.kafkaBinding` de la version Delta 0 et le fichier `_stokora_kafka_binding` de chaque checkpoint enregistrent la liaison. Un ancien registre sans liaison ou un checkpoint déjà utilisé sans marqueur entraîne un arrêt. Le broker, la table et le checkpoint doivent être cohérents à la reprise. Un checkpoint neuf peut rejoindre la même table liée. Les lignes Spark transportent le nom et la position, et l'UUID est vérifié séparément par AdminClient. La consultation du broker et la transaction Delta sont deux opérations distinctes.

Pour changer d'incarnation, arrêter l'écrivain, conserver en lecture seule l'ancienne table et ses checkpoints, vérifier les identifiants par AdminClient sur la source prévue et faire approuver la réconciliation des coordonnées du connecteur et des ventes. Affecter ensuite un nouveau volume nommé et un checkpoint neuf, vérifier les offsets disponibles et l'intégrité sur ce nouveau registre avant bascule. Les métadonnées existantes ne sont jamais réécrites. Une restauration Kafka qui conserve les mêmes identifiants nécessite une revue de continuité et les conflits d'octets restent bloquants. L'écriture directe par un autre processus, la manipulation du volume par un administrateur et la sécurité en transit sur un réseau non fiable dépassent la garantie locale.
