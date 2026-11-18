# Cycle de vie Docker

Le point d’entrée est scripts/stokora.ps1. Il référence infra/compose/local.yml et utilise le projet stokora-local quel que soit le répertoire courant. Chaque service conserve un nom fixe. Les procédures antérieures dans infra/compose documentent les configurations de qualification historiques.

| Action du script | Commande Compose équivalente après docker compose -p stokora-local -f infra/compose/local.yml |
|---|---|
| build | build |
| up | up -d --wait --wait-timeout 300 |
| status | ps |
| generate | exec -T postgres stokora generate |
| test | exec -T spark stokora test |
| pipeline | exec -T spark stokora pipeline |
| recover | exec -T spark stokora recover |
| forecast | exec -T spark stokora forecast |
| logs | logs --tail 100 |
| stop | stop |
| restart | stop, puis up -d --wait --wait-timeout 300 |

Le pipeline et la prévision utilisent un verrou et un répertoire temporaire par invocation. PostgreSQL publie un indicateur de disponibilité après les migrations. Kafka attend le broker authentifié et configure les topics et ACL. Connect attend une tâche Debezium active avant de laisser démarrer la session Spark. Le mécanisme de santé observe le rôle, et le superviseur termine si son service enfant s’arrête.

L’arrêt standard conserve les conteneurs, les volumes et les images de reprise. Le nettoyage historique utilise les identifiants archivés et une autorisation ciblée. Pour les diagnostics, consulter les logs de tous les services et l’état détaillé de chaque sonde Docker.
