# Profils Docker Compose

| Profil | Services cibles | Plafond proposÃƒÂ© |
|---|---|---|
| core | PostgreSQL, Kafka KRaft, Debezium Connect | 3 vCPU / 3072 MiB |
| processing | Spark master/worker, jobs Delta et Great Expectations | 4 vCPU / 5120 MiB |
| orchestration | Airflow scheduler/API, MLflow, mÃƒÂ©tadonnÃƒÂ©es dÃƒÂ©diÃƒÂ©es | 3 vCPU / 4608 MiB |
| app | FastAPI, Next.js, Superset, proxy HTTPS | 2 vCPU / 2560 MiB |
| monitoring | Prometheus, Grafana | 1 vCPU / 1024 MiB |
| security | Fournisseur OIDC, Keycloak ÃƒÂ  ÃƒÂ©valuer | 1 vCPU / 1024 MiB |

Le socle Compose dÃƒÂ©clare PostgreSQL. La surcharge `core.yml` complÃƒÂ¨te le profil avec Kafka et Connect sur le rÃƒÂ©seau interne sans ports hÃƒÂ´te. Les limites du profil sont de 3 vCPU et 2944 MiB, dont 768 pour PostgreSQL, 1024 pour Kafka et 1152 pour Connect. `processing.yml` lance Spark en mode local ÃƒÂ  deux threads dans un projet sÃƒÂ©parÃƒÂ©, sans rÃƒÂ©seau, avec 3 GiB et 2 vCPU. Les quatre autres profils restent rÃƒÂ©servÃƒÂ©s. Voir le [guide Bronze](../../docs/bronze-batch.md).

Cible locale initiale pour Docker/WSL de 9 GiB et 8 vCPU, ÃƒÂ  vÃƒÂ©rifier sur la machine. AprÃƒÂ¨s qualification, dÃƒÂ©marrer d'abord core puis soit processing, soit orchestration. Suspendre les workloads lourds avant une autre phase. Mesurer la mÃƒÂ©moire WSL et `docker stats --no-stream` ÃƒÂ  froid, en charge et aprÃƒÂ¨s redÃƒÂ©marrage. Le GPU est optionnel. Les plafonds nominaux cumulÃƒÂ©s excÃƒÂ¨dent la capacitÃƒÂ© locale.


## DÃƒÂ©marrage et preuve CDC

Avec Docker Desktop en mode Linux, depuis la racine, exÃƒÂ©cuter `python scripts/smoke_cdc.py`. Il crÃƒÂ©e le projet isolÃƒÂ© `stokora-core-smoke` et un dossier de secrets local, attend la santÃƒÂ© de PostgreSQL et Kafka, prÃƒÂ©pare topics et ACL, enregistre le connecteur et vÃƒÂ©rifie les transactions synthÃƒÂ©tiques. Le test arrÃƒÂªte ses conteneurs et garde ses volumes. Pour une opÃ©ration rÃ©pÃ©tÃ©e, reprendre le projet existant. Voir le [guide du cycle de vie](../../reports/infrastructure/DOCKER_LIFECYCLE_GUIDE.md). Les migrations s'appliquent uniquement ÃƒÂ  une base initialement vide.

Pour redÃƒÂ©marrer un projet de test dÃƒÂ©jÃƒÂ  provisionnÃƒÂ© et vÃƒÂ©rifier la santÃƒÂ©, reprendre le nom exact affichÃƒÂ© par le smoke. Laisser les fichiers secrets locaux en place et protÃƒÂ©ger leurs ACL hÃƒÂ´te, en particulier sous Windows.

```powershell
$project = 'stokora-core-smoke'
$env:CORE_SECRET_DIR = (Resolve-Path "config/secrets/$project").Path.Replace('\','/')
$env:POSTGRES_PASSWORD_FILE = "$env:CORE_SECRET_DIR/postgres_password"
docker compose -p $project --env-file config/.env.example -f infra/compose/compose.yaml -f infra/compose/core.yml --profile core up -d --wait
python scripts/smoke_cdc.py --project $project --health
docker compose -p $project --env-file config/.env.example -f infra/compose/compose.yaml -f infra/compose/core.yml --profile core stop
```

Une publication explicite expose seulement les ventes. La clÃƒÂ© Kafka composite vient de la clÃƒÂ© primaire PostgreSQL. Le topic brut `stokora.operational.sales_events` conserve les enveloppes natives Debezium `r/c/u/d`, avec `before`, `after`, LSN et `ts_ms`. Un tombstone suit le delete. Une lecture de snapshot initialise un ÃƒÂ©tat. Elle ne reprÃƒÂ©sente aucune nouvelle vente. Le contrat mÃƒÂ©tier STEP 02 reste applicable aprÃƒÂ¨s normalisation, ÃƒÂ  une ÃƒÂ©tape ultÃƒÂ©rieure.

Kafka stocke donnÃƒÂ©es, mÃƒÂ©tadonnÃƒÂ©es KRaft et offsets/configuration/ÃƒÂ©tat Connect sur son volume. `stokora.quarantine` est rÃƒÂ©servÃƒÂ© ÃƒÂ  la quarantaine aval. Les erreurs source arrÃƒÂªtent la tÃƒÂ¢che, avec `errors.tolerance=none`. Les ACL ferment l'accÃƒÂ¨s par dÃƒÂ©faut, le compte Connect accÃƒÂ¨de ÃƒÂ  ses topics internes et publie sur le topic brut, et le compte d'observation lit le brut. L'administration reste dans Docker. Kafka SASL/PLAIN et le REST Connect HTTP reposent sur le rÃƒÂ©seau de prototype privÃƒÂ©. Pour un dÃƒÂ©ploiement, protÃƒÂ©ger le transport par TLS, restreindre le rÃƒÂ©seau administratif et changer la gestion des secrets. Les rÃƒÂ´les de service et la base de dÃƒÂ©monstration sont rÃƒÂ©servÃƒÂ©s ÃƒÂ  un dÃƒÂ©veloppeur de confiance.

Surveiller `active`, `wal_status`, la diffÃƒÂ©rence avec `restart_lsn` et celle avec `confirmed_flush_lsn`. Un slot inactif retient le WAL. Le plafond `max_slot_wal_keep_size=512MB` s'applique au checkpoint et un dÃƒÂ©passement peut invalider le slot. Selon la rÃƒÂ©tention WAL et Kafka, la reprise peut demander un resnapshot explicite et une rÃƒÂ©conciliation. Kafka mono-broker et partition unique ne dÃƒÂ©montrent aucune haute disponibilitÃƒÂ©. Utiliser une dÃƒÂ©duplication aval pour la livraison au moins une fois.
