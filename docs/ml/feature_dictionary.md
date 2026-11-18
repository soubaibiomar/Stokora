# STEP 07 — Dictionnaire des variables

Le flux lit le mart Gold `sales_daily` du snapshot synthétique approuvé et ajoute une fixture de séries longues dans `simulator:step07-v1`. Le même identifiant de produit et de site peut exister chez deux tenants. Le partitionnement comprend le tenant, le dataset, le site et le produit. Chaque ligne de variables ajoute l'origine UTC. Les prévisions ajoutent un horizon entier 1–7 et une méthode.

| Champ | Définition |
|---|---|
| `origin` | Début de journée UTC. Seules les ventes de dates strictement antérieures sont visibles. Première prévision le lendemain de la première observation connue pour la série. |
| `lag_7`, `lag_14`, `lag_28`, `prior_week` | Valeurs à dates calendaires exactes. La semaine antérieure est ordonnée de J−7 à J−1. |
| `mean_7`, `mean_28`, `stddev_28`, `trend_7_vs_28` | Moyennes des observations, écart-type population, différence des deux moyennes. Les deux fenêtres s'arrêtent à J−1. |
| `last_observed`, `observed_days_28`, `missing_days_28`, `zero_days_28`, `zero_fraction_28` | Dernière observation dans les 28 jours, couverture, intermittence. Une journée manquante reste nulle, une vente observée de zéro est comptée. |
| `historical_price` | Dernier prix dans les 28 jours précédents. La fixture affirme sa disponibilité à la date correspondante. Le prix Gold reste nul faute d'historique de disponibilité vérifié. |
| `target_date`, `target_weekday`, `target_month` | Date J + horizon − 1, jour Spark (dimanche = 1), mois. Champs déterministes dans le fichier des prévisions, joignables par origine et horizon. |
| `actuals`, `unit_sales_next_7d` | Étiquettes séparées des variables. Le total est nul au sens SQL si une des sept dates manque. Les métriques journalières utilisent les valeurs observées et indiquent leur couverture. |
| `simulated`, `unit`, `run_id` | Chaque résultat est synthétique. Les quantités sont en unité `each`. Le manifeste identifie le snapshot, ses frontières, la version des programmes et le contenu de la fixture. |

Frontières figées en 2024, apprentissage à partir du 6 janvier, validation en avril, test en mai jusqu'au 31 inclus. Toute origine dont la cible franchit une frontière est retirée. Une cible peut être évaluée à plusieurs horizons issus d'origines différentes. Aucun ajustement sur le test gelé ni transformation apprise.

Le calcul est valide selon le snapshot épinglé. Les corrections créent un nouvel identifiant de snapshot et changent les variables qui en dépendent. Les anciens résultats restent reproductibles depuis leur version. Le Gold ne fournit pas un historique complet de disponibilité au moment de chaque ancienne origine. Les promotions, événements spéciaux et catégories sans références historisées restent indisponibles. Aucun champ client, fournisseur ou niveau de stock ne passe dans le jeu de variables. Les métriques par catégorie attendent une dimension fiable.

Les variables et étiquettes Parquet, prévisions, métriques JSON par série/site/globales et le manifeste sont publiés ensemble dans le volume dédié. Le score signé est prévision − observation. WAPE = somme des erreurs absolues / somme des ventes observées, avec résultat nul si le dénominateur vaut zéro. Une prévision saisonnière utilise le même jour de la semaine précédente et retombe sur la dernière observation ou zéro. La moyenne mobile utilise sept jours passés avec le même repli.

Les nouveaux runs publient aussi `coverage/` et `coverage.json`. La preuve distingue clés scorées, clés après la dernière observation, dates manquantes au sein de l’historique et prévisions absentes. Le type de repli appartient à la prévision. Une étiquette manquante reste nulle.

Le test pré-STEP 08 exerce séparément une fixture versionnée par date métier, disponibilité et révision pour les ventes, prix et calendrier. Les scores de catégorie utilisent une autre fixture, avec jointure sur la clé de série complète. Ces tests ne confèrent pas de garanties de disponibilité historique ou de catégories au Gold approuvé. Audit (archive de qualification locale)
