# Batch M5 vers Bronze Delta

Utiliser Docker Desktop Linux, depuis la racine PowerShell. Garder le core CDC arrêté pendant les mesures. `./scripts/fetch_delta.ps1` télécharge les deux JAR depuis Maven Central et vérifie leurs SHA-256. L'image Spark est fixée par digest. Le service ne démarre ni Jupyter ni interface Spark, n'a pas de réseau et tourne sans privilège ajouté.

```powershell
docker compose -f infra/compose/processing.yml --profile processing run --rm --name stokora-bronze-import spark --fixture
docker compose -f infra/compose/processing.yml --profile processing run --rm --name stokora-bronze-review --entrypoint python spark /workspace/tests/integration/batch/verify_bronze.py
```

Pour M5 réel, suivre le [guide d'acquisition](../data/README.md), déposer les trois CSV autorisés dans `data/local/m5`, puis remplacer `--fixture` par `--licensed --retrieved-on YYYY-MM-DD` avec la vraie date. `--evaluation` sélectionne le fichier à 1 941 jours. Les entrées et dépendances du conteneur sont en lecture seule. Les cibles par défaut `m5-fixture` et `m5-licensed` sont séparées. `--target nom-local` isole une éventuelle expérience.

## Registre, quarantaine et relecture

`m5_bronze` est une table Delta append-only sur volume local. Sous un verrou de fichier Linux, le job copie les sources, refuse une entrée non UTF-8 ou modifiée pendant la copie, calcule les empreintes de la copie et valide les en-têtes. Un commit Delta contient les enregistrements `load` du registre, `raw` acceptés et `reject` de quarantaine. Une entrée `load` permet un audit par empreinte, source, version, colonnes, taille, origine et exécution. La ligne brute reste sous forme CSV sans fin de ligne. Les erreurs de ligne restent dans le volume privé, avec un code motif. Un en-tête incompatible bloque le lot complet.

Les trois CSV sont requis. Un fichier de zéro octet ou composé uniquement d'espaces est rejeté. La source des ventes doit produire au moins une ligne acceptée par le classificateur Bronze. Un CSV de ventes limité à son en-tête ou rempli exclusivement de lignes invalides est rejeté avant consultation du registre. Un rejet de validation fournit sur stderr un objet JSON avec `status`, `code`, `source_file` et `message`, et retourne 2. Les codes spécifiques sont `EMPTY_INPUT`, `NO_ACCEPTED_SALES` et `SCHEMA_ERROR`. Aucun chargement ni commit n'est publié pour ce lot. Les rejets restent en quarantaine dans les lots qui satisfont le minimum requis.

La clé logique d'une ligne est `(dataset, source_table, schema_version, source_hash, source_line)`. Le registre évite toute seconde écriture d'un fichier identique. Pour relire, filtrer `record_type = 'raw'` et choisir l'empreinte de source voulue pour chaque table. Préciser le fichier sales validation ou evaluation pour éviter des périodes qui se recouvrent. Une version Delta précédente peut être relue avec `versionAsOf`, sans écraser le journal ni restaurer physiquement une donnée. Il n'y a aucune écriture Silver ou Gold.

Deux partitions d'exécution et au plus deux fichiers de sortie par lot dans le test. Aucune partition de table par produit. Prévoir 3 GiB pour le conteneur, dont 1 GiB pour le driver JVM, et au moins 64 MiB + trois fois la taille des entrées sur le volume. C'est un seuil conservateur de préflight, avant toute écriture, indépendant de la compression finale. Ne pas saturer ni nettoyer un volume existant pour tester l'incident. La preuve utilise le tmpfs de 16 MiB du conteneur. Le volume nommé reste présent après les jobs. Restreindre l'accès Docker et les ACL hôte. Le verrou protège les exécutions locales coopérantes, pas les écrivains distribués ni les utilisateurs Docker privilégiés.

Pour mesurer 500 ou 1 000 séries synthétiques à 1 913 jours, reprendre la commande de revue avec `--series 500` ou `--series 1000` après le nom du script. Lancer une taille à la fois. Aucun GPU n'est nécessaire.
