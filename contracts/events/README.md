# Événements transactionnels

Le [schéma v1](v1.schema.json) utilise JSON Schema 2020-12. Une enveloppe et cinq branches discriminées partagent les métadonnées. Le format se valide localement sans registre. Garder v1 stable. Si un changement modifie les champs fermés ou leur sens, publier une nouvelle version et définir une migration explicite.

| Champ | Contrat |
|---|---|
| event_id, tenant_id, site_id, source | Identifiants opaques de 1 à 80 caractères. Portée de déduplication tenant, site, source et event_id. |
| origin_timestamp, ingestion_timestamp | UTC `YYYY-MM-DDTHH:MM:SSZ`. L'ingestion est égale ou postérieure à l'origine. |
| type, operation, version | Type discriminé, opération contrôlée, version de l'enveloppe égale à 1. |
| is_simulated, synthetic_scenario_id | Scénario obligatoire si synthétique, nul autrement. |
| payload.revision | Révision métier distincte de la version du schéma. |
| payload.quantity / quantity_signed | Quantité entière positive pour vente ou approbation. Mouvement signé non nul, négatif pour sortie vente, positif pour ouverture, réception ou retour. |
| payload.unit_price_minor, currency | Montant entier en centimes de la devise explicite. Devises v1 USD, MAD, GBP, EUR. |

| Type | Transition |
|---|---|
| sale_created | Enregistre une vente en révision 1. |
| sale_updated | Remplace quantité et prix à la révision suivante. Produit et devise stables. |
| sale_cancelled | Annule une vente active et ramène ses unités nettes à zéro, avec motif et révision. |
| stock_movement | Enregistre un delta signé par produit, unité et motif. |
| purchase_order_approved | Représente une décision humaine avec identifiant du décideur. L'enveloppe seule ne prouve pas l'identité. Le générateur n'émet aucune approbation. |

`validate_batch` reçoit au plus 10 000 objets JSON déjà décodés et un périmètre tenant/site établi par l'appelant. Il retourne les événements acceptés, les unités nettes par vente, le nombre de rejeux et une quarantaine locale contenant seulement numéro de ligne et code motif. Un doublon avec horodatage d'ingestion différent est reconnu. Si le contenu métier diffère, la collision est rejetée. Les ventes sont réconciliées dans l'ordre du lot, avec création avant correction ou annulation. L'intégration future OIDC/SQL doit établir le périmètre authentifié et persister états et déduplication au-delà du lot.

## Tables de simulation

| Table | Grain et champs métier |
|---|---|
| initial_inventory | Stock initial par scénario, tenant, site, produit et date, unité each et devise MAD. |
| suppliers | Relation par scénario, tenant, site, produit et fournisseur, délai en jours. |
| events | Transaction par portée et identifiant, avec provenance et scénario. |
| manifest | Résumé par graine et version, avec effectifs des tables. |
