# Contrat canonique de données

- Vente quotidienne M5 : `source_dataset, store_id, product_id, business_date`. Pour les flux d'organisation, partitionner et contrôler tout accès par `tenant_id, site_id` en plus des clés source.
- Événement canonique : `source_dataset, event_id` dans son périmètre d'organisation/site. Conserver opération, identifiant de ligne et ordre source pour CDC, topic/partition/offset et horodatage UTC. Un update remplace ou corrige, un delete annule, un snapshot est traité explicitement.
- Bronze conserve source, hash/offset, version et date d'import. Silver contrôle types, unités, clés et dédoublonnage via Great Expectations, enregistre les rejets en quarantaine. Gold publie agrégats nets, calcul/version/fraîcheur, avec test de rejeu. Checkpoint distinct par requête.
- Le stockage réel et chaque ligne synthétique ont une provenance distincte. Les interfaces de décision exposent `forecast_run_id`, unités, données utilisées, statut et version de ligne. Les KPI nécessitant des coûts ou balances absents restent indisponibles.

## Dictionnaire des faits historiques

| Champ | Type et sens |
|---|---|
| source_dataset | `m5` ou `m5_fixture`, inclus dans la clé. |
| store_id, product_id | Identifiants locaux issus de store_id et item_id. |
| business_date | Date ISO issue de la clé calendrier d_N. |
| quantity, unit | Entier non négatif en unités `each`, champ CSV vide si valeur absente. |
| sell_price, currency | Décimal en texte préservé, USD, prix par magasin/produit/semaine. Vide si indisponible. |
| is_simulated, source_release | Marqueur de fixture et fichier source sans extension. |

Consulter [le dictionnaire transactionnel](events/README.md) pour les événements et la simulation.
