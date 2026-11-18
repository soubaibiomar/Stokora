# ADR-01 — Lambda

**Statut** — Acceptée pour le socle de conception, 2026-11-16.

## Contexte

L'historique M5 arrive en fichiers et les transactions via CDC. Le backfill Kappa imposerait de convertir et rejouer tout l'historique via Kafka.

## Décision et conséquences

Distinguer le batch et le flux, partager le contrat canonique et réconcilier en Gold. Démontrer que replay et corrections préservent les totaux.
