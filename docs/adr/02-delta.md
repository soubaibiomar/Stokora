# ADR-02 — Delta Lake et couches Bronze, Silver, Gold

**Statut** — Acceptée pour le socle de conception, 2026-11-16.

## Contexte

Besoin de provenance brute, qualité reproductible et mise à jour des faits. Des fichiers Parquet seuls demanderaient de reconstruire les mécanismes transactionnels.

## Décision et conséquences

Conserver Bronze brut, valider et isoler les rejets avant Silver et réconcilier Gold avec MERGE. Stockage local pour le prototype. Tester les reprises et le protocole Delta avant tout changement de stockage.
