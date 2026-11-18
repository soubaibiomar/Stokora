# ADR-03 — PostgreSQL comme autorité opérationnelle

**Statut** — Acceptée pour le socle de conception, 2026-11-16.

## Contexte

Les ventes et mouvements demandent contraintes et transactions. Kafka distribue des événements et Delta sert le calcul analytique.

## Décision et conséquences

Enregistrer une transaction métier une seule fois, appliquer WAL/CDC et conserver ordre et opérations. Séparer identités bootstrap, API, CDC, BI et audit.
