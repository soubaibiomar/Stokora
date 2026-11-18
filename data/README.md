# Données locales

## Acquisition et exécution M5

Se connecter à son propre compte Kaggle, consulter les conditions et les accepter si elles permettent l'usage prévu. Télécharger manuellement `calendar.csv`, `sell_prices.csv` et `sales_train_validation.csv`, puis extraire les fichiers sous `data/local/m5/`. Conserver la vraie date de récupération. Pour le jeu optionnel `sales_train_evaluation.csv`, ajouter `--evaluation`. [Données M5](https://www.kaggle.com/competitions/m5-forecasting-accuracy/data) · [Règles de la compétition](https://www.kaggle.com/competitions/m5-forecasting-accuracy/rules)

Depuis la racine avec Python 3.11 ou ultérieur, définir `$dateDeRecuperation` au format YYYY-MM-DD et choisir une sortie neuve.

```powershell
python -m pipelines.adapters.m5 --source data/local/m5 --retrieved-on $dateDeRecuperation --output data/local/m5-validation
python -m pipelines.adapters.m5 --fixture --output data/local/m5-fixture
```

Le CLI utilise uniquement les fichiers locaux. Une entrée absente provoque un arrêt avec indication du mode fixture explicite. Chaque import publie `manifest.json` et `sales.csv`, avec SHA-256 des sources, taille, colonnes, grain, date, conditions source, profil et hash des faits. Prévoir plusieurs GiB libres pour les faits et l'index prix temporaire. Conserver sources, sorties et manifestes réels hors Git et des rapports diffusés. Dans le dépôt, la sortie licenciée est confinée à `data/local/`. Le [manifeste de fixture](manifests/m5-fixture.json) documente les six faits simulés.

## Interprétation et sources indépendantes

Déposer les données autorisées sous ce répertoire ignoré par Git. Pour chaque import, consigner origine, conditions de réutilisation, SHA-256, date d'extraction, grain, schéma et version du transformateur.

M5 mesure les ventes observées par produit/site/jour. Les ruptures et la demande non satisfaite restent inconnues. Stock, délai, coût et réservations de démonstration portent `is_simulated=true`. Les simulations servent à tester le calcul sans mesurer des économies réelles. Garder les prix en devise source et les fichiers Kaggle hors du dépôt. UCI, Favorita et Olist restent des sources distinctes, sans jointure sur leurs identifiants locaux. Préserver les unités et la devise explicite, utiliser des décimaux pour les montants.

Les prix hebdomadaires sont repris en USD. Une quantité ou un prix manquant reste vide. Une vente à zéro est distincte. Examiner les compteurs de semaines prix sans calendrier et de ventes positives sans prix avant l'analyse.

UCI Online Retail II est un banc indépendant facultatif pour le schéma et les annulations. Conserver le préfixe d'annulation de facture, appliquer l'attribution CC BY 4.0 et écarter CustomerID. [UCI Online Retail II](https://archive.ics.uci.edu/dataset/502/online+retail+ii)
