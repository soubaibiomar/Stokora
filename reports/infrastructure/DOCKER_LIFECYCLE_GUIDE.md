# Cycle de vie Docker

Depuis la racine, utiliser PowerShell 7 et Docker Desktop en mode Linux. Les noms ci-dessous identifient des environnements synthétiques distincts. Les données CDC et les snapshots de qualification persistent sur la station d'origine.

| Opération | Commande depuis la racine |
|---|---|
| Inspecter le core de développement restauré | `./scripts/core_lifecycle.ps1 inspect dev` |
| Démarrer ou arrêter ce core | `./scripts/core_lifecycle.ps1 start dev` ou `./scripts/core_lifecycle.ps1 stop dev` |
| Inspecter le staging sécurité | `./scripts/core_lifecycle.ps1 inspect staging` |
| Démarrer ou arrêter le staging déjà provisionné | `./scripts/core_lifecycle.ps1 start staging` ou `./scripts/core_lifecycle.ps1 stop staging` |
| Exécuter le contrôle temporel et libérer le worker | `./scripts/forecast_fixture.ps1 run` |
| Ouvrir, inspecter puis fermer un worker de test | `./scripts/forecast_fixture.ps1 start`, `./scripts/forecast_fixture.ps1 inspect`, `./scripts/forecast_fixture.ps1 stop` |
| Utiliser le second emplacement indépendant | Ajouter `stokora-test-review` après l'action. |
| Inventorier les projets | `docker compose ls -a` puis consulter les CSV du dernier inventaire. |
| Vérifier les fichiers historiques | `./docs/evidence/sec-01-staging-and-spark-closure/check-protected.ps1` |

Les commandes du core nécessitent l'inventaire local d'enrôlement, les trois conteneurs d'origine et leurs volumes exclusifs. Elles contrôlent les identités, le réseau interne, les montages et la santé. Elles reprennent les conteneurs existants avec les mêmes images. Le développement utilise le projet TLS historique `stokora-security-final`. Le staging utilise `stokora-sec01-owner-staging`. Un redémarrage du core écrit dans ses journaux et peut reprendre le CDC. Réserver ces opérations aux interventions autorisées sur la fixture correspondante. La promotion en déploiement demeure bloquée.

Le test hors ligne utilise le profil forecast existant, son image de qualification locale, les JAR Delta et les volumes Gold/GX de la station de référence. Sa racine est en lecture seule. Les écritures vont en mémoire privée. La bibliothèque Snappy se charge dans un montage de 8 Mio dédié. Un worker se termine au bout d'une heure. `run` attend le résultat du test puis la disparition de son propre conteneur. Les deux emplacements sont réservés au même utilisateur Docker sous Windows. Un mutex sérialise les opérations du même emplacement sur la session Windows. Un second appel `start` reprend le même worker.

Pour un smoke CDC initial avec historique vierge, consulter le guide core. Les anciennes campagnes indépendantes ont conservé plusieurs bases, offsets et checkpoints parce qu'une seconde expérience devait partir d'une identité distincte. Utiliser le worker jetable pour les régressions ordinaires. Pour libérer celui-ci, employer `stop`. Le nettoyage des ressources historiques exige un nouvel inventaire et une liste d'identifiants exacts. Le script de nettoyage daté de cette campagne est une pièce d'audit à usage unique.
