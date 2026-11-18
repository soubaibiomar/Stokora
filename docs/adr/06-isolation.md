# ADR-06 — Isolation par organisation et site

**Statut** — Acceptée pour le socle de conception, 2026-11-16.

## Contexte

Les filtres d'interface seuls ne contraignent ni API ni SQL ni BI. Des frontières réseau réduisent l'exposition des services techniques.

## Décision et conséquences

Dériver les droits d'une identité OIDC vérifiée, refuser par défaut, scoper stockage et requêtes par tenant/site et n'exposer que le proxy autorisé. Tester deux organisations, deux sites et plusieurs rôles.
