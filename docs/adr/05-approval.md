# ADR-05 — Approbation humaine des recommandations

**Statut** — Acceptée pour le socle de conception, 2026-11-16.

## Contexte

Quantité, délai et coût peuvent être incertains ou simulés. Un envoi automatique transfèrerait cette incertitude vers une opération irréversible.

## Décision et conséquences

Versionner prévision, stock, hypothèses et état de la proposition. Vérifier autorisation, transition, version et idempotence dans une transaction qui écrit aussi l'audit. Aucun envoi automatique V1.
