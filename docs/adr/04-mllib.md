# ADR-04 — Spark MLlib et baseline temporelle

**Statut** — Acceptée pour le socle de conception, 2026-11-16.

## Contexte

La prévision tabulaire utilise les mêmes outils distribués que le pipeline. Un modèle profond ajouterait charge et dépendances avant d'établir une baseline.

## Décision et conséquences

Comparer à un naïf saisonnier avec fenêtres chronologiques et jeu test gelé. Ajuster les transformations sur train seul. Conserver métriques et provenance dans MLflow.
