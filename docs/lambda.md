# Silver, Gold et réconciliation Lambda

STEP 06 traite la fixture M5 versionnée et un registre CDC STEP 05 lié à Kafka. Le conteneur Spark monte Bronze, contrats, code et dépendances en lecture seule. Il écrit uniquement dans le volume dérivé privé. Les fichiers sous `docs/evidence/step-06/` documentent l'installation GX et la validation.

```powershell
docker compose -f infra/compose/lambda.yml run --rm -T --name stokora-step06-spark spark --batch-version 0 --cdc-version 1
```

Les deux versions sont obligatoires. Les volumes externes dans Compose sont ceux de la campagne STEP 04 et du gate STEP 05. Pour utiliser un autre registre compatible, sélectionner son volume par surcharge Compose. Le dossier de sortie contient une table Delta `lambda_snapshot`. Les lignes `quality` identifient les snapshots publiés. Les consommateurs choisissent explicitement un identifiant de snapshot. La date `published_at` mesure la publication, et `refreshed_source` provient de Bronze.

| Kind | Grain |
|---|---|
| `silver_batch` | Source, tenant synthétique M5, magasin, produit, jour |
| `silver_cdc` | Source CDC vérifiée et reçus de mutations acceptées, y compris suppressions |
| `silver_sales` | Vente active CDC ou cellule historique canonique |
| `quarantine` | Référence Bronze et motif technique |
| `sales_daily`, `sales_weekly` | Source, tenant, site, produit, jour ou lundi ISO |
| `products`, `stores` | Identifiant de référence par tenant et source |
| `product_store_metrics` | Source, tenant, site, produit |
| `forecast_features` | Grain quotidien et valeur du jour précédent si consécutif |
| `stock_movement`, `stock_snapshot_simulated` | Événement du générateur ou somme du journal sur sa journée d'ouverture |
| `quality` | Version du traitement, tables et versions Bronze, comptes et règles GX |

Les quantités sont des entiers en unités, les dates sont typées et les prix unitaires sont décimaux avec devise. Les sources M5 et CDC conservent leurs espaces d'identifiants. `--mapping` lit des demandes JSON de la forme `{"batch_event_id":"FIX_A:FIX_P:2024-01-06","cdc_key":["SIM_A","SIM_SITE","snapshot"],"approval_id":"fixture-jan06"}`. Le registre local déployé en lecture seule décide de l'autorisation. Cette approbation couvre la table Bronze historique de la campagne, l'identité du topic CDC vérifié, les deux tenants, les sites, produits et événements, la date initiale et deux unités. Elle expire le 1er janvier 2027 UTC. Une autre incarnation Kafka ou table historique exige une nouvelle politique déployée. La demande et les événements ne peuvent créer aucune approbation. Les droits de l'approbateur, la version, la période de validité et la révocation sont vérifiés avant toute publication ou reconnaissance d'un rejeu. Une correction et une suppression utilisent les mêmes clés et jour approuvés. Le snapshot qualité conserve les demandes et l'empreinte du registre. Le lecteur applicatif refuse les anciens snapshots mappés sans preuve de la politique active.

Le calendrier associe une cellule `d_N` à un couple date ISO, semaine dans le manifeste actif de `m5_fixture`. Les définitions identiques convergent et gardent leurs références. Les ventes liées à une référence manquante, invalide ou contradictoire sont isolées avec référence à la cellule et candidats connus. Les conflits de prix valides sont aussi isolés. Le nouveau manifeste d'une correction approuvée sélectionne une version complète distincte. Les anciennes versions et leurs journaux restent lisibles. Un snapshot de qualité partielle publie en une transaction les faits valides, leur quarantaine et le rapport qualité. Une demande de correspondance refusée interrompt toute la publication.

Les mutations CDC se déroulent par LSN puis position Kafka. La comparaison des images avant prévient les corrections incohérentes. Une rediffusion identique converge et les tombstones Kafka n'affectent aucune vente. La date métier pilote les marts, le temps source reste distinct de l'ingestion et de la publication. Le stock vient du scénario du générateur, avec couverture calculée à cinq unités par jour et seuil de surstock à vingt jours. Ce scénario ne partage aucune clé de stock avec M5.

Toute évolution du schéma ou des règles exige une nouvelle version de spécification et une validation de lecture. Une seule transaction Delta écrit les catégories et la ligne qualité. Les reprises sur les mêmes IDs de table, versions et correspondances conservent l'identifiant publié. Un verrou local protège les écrivains coopérants. L'accès applicatif utilise une identité fournie par l'hôte de confiance et vérifie le tenant dans l'autorisation puis dans le filtre Spark. L'hôte administrateur garde l'accès direct au volume privé.
