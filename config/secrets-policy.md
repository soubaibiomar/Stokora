# Secrets et accès techniques

Les valeurs actives restent dans un gestionnaire de secrets ou, pour le développement local, dans des fichiers à ACL restreintes sous `config/secrets/`. Pour PostgreSQL, Compose monte le fichier dans le seul service consommateur et transmet son chemin via `POSTGRES_PASSWORD_FILE`. Les secrets Compose locaux sont des fichiers montés et nécessitent la protection du stockage hôte. L'exemple d'environnement contient uniquement des chemins et des références d'images.

Le compte bootstrap initialise la base. Prévoir des comptes distincts pour API, CDC, calcul, BI et audit, des droits minimaux par schéma et des accès temporaires pour la CI. Masquer les valeurs dans toutes les preuves. En cas d'exposition, révoquer et faire tourner la valeur, examiner les accès, puis traiter le dépôt avec accord explicite avant toute réécriture publiée.

Exécuter Gitleaks sur les fichiers concernés (y compris non suivis via la liste Git) et sur l'historique lorsqu'il existe. Le test négatif place un jeton factice dans un répertoire temporaire hors du dépôt et exige une détection réelle et une sortie expurgée. Les règles du scanner et `git check-ignore` couvrent des risques complémentaires.
