# TP Noté Airflow — Data Platform Santé Publique ARS Occitanie
## Auteur
- Nom : RADAL
- Prénom : Noah
- Formation : MIA 26.2
- Date : 13/04/2026

## Prérequis
- Docker Desktop >= 4.0
- Docker Compose >= 2.0
- Python >= 3.11 (pour les tests locaux)

## Instructions de déploiement
### 1. Démarrage de la stack
```bash
# Cloner le projet
git clone https://github.com/noahrd0/tp_airflow_sante_publique_ars.git ars_occitanie
cd ars_occitanie/ars-epidemio

# Copier le fichier d'environnement
cp .env.example .env

# Créer le répertoires de données brut
mkdir -p data/ars/raw

# Si problème de permissions uniquement
sudo chown -R $(id -u):$(id -g) ./data/ars
chmod -R 777 ./data/ars

# Démarrer la stack
docker compose build
docker compose up -d

# Vérifier que tous les services sont up
docker compose ps
```

### 2. Configuration des connexions et variables Airflow

#### Connexion PostgreSQL ARS

Via l'UI Airflow (`http://localhost:8080`) → **Admin → Connections → +** :

| Champ | Valeur |
|-------|--------|
| Connection Id | `postgres_ars` |
| Connection Type | `Postgres` |
| Host | `postgres-ars` |
| Schema | `ars_epidemio` |
| Login | `postgres` |
| Password | `postgres` |
| Port | `5432` |

#### Variable Airflow

Via l'UI Airflow (`http://localhost:8080`) → **Admin → Connections → +** :

| Clé | Valeur |
|-----|--------|
| `semaines_historique` | `12` |
| `seuil_alerte_incidence` | `150` |
| `seuil_urgence_incidence` | `500` |
| `seuil_alerte_zscore` | `1.5` |
| `seuil_urgence_zscore` | `3.0` |
| `departements_occitanie` | `["09","11","12","30","31","32","34","46","48", "65", "66", "81", "82"]` |
| `syndromes_surveilles` | `["GRIPPE","GEA","SG","BRONCHIO","COVID19"]` |
| `archive_base_path` | `/data/ars` |

### 3. Démarrage du pipeline

Le DAG est configuré avec `catchup=True` et `start_date=2024-10-01`. Une fois le DAG activé (unpause), le scheduler crée automatiquement les runs pour chaque lundi depuis octobre 2024 (début de la saison grippale 2024-2025) jusqu'à aujourd'hui.

```bash
# Activer le DAG
docker exec -it ars-epidemio-airflow-webserver-1 airflow dags unpause ars_epidemio_dag
```

Suivi dans l'UI : `http://localhost:8080` → DAGs → `ars_epidemio_dag` → Graph View


## Architecture des données

Les données brutes et rapports sont organisés par année et semaine ISO dans un bind mount Docker (`./data/ars` → `/data/ars`) :

```
data/ars/
├── raw/                                    # Données brutes collectées
│   └── <année>/
│       └── S<xx>/
│           └── sursaud_<année>-S<xx>.json
└── rapports/                               # Rapports générés
    └── <année>/
        └── S<xx>/
            └── rapport_<année>-S<xx>.json
```

### Schéma PostgreSQL

La base `ars_epidemio` contient 5 tables :

| Table | Description | Clé d'unicité |
|-------|-------------|---------------|
| `syndromes` | Référentiel des 5 syndromes surveillés (GRIPPE, GEA, SG, BRONCHIO, COVID19) | `code` |
| `departements` | 13 départements d'Occitanie | `code_dept` |
| `donnees_hebdomadaires` | Données IAS® agrégées par semaine et syndrome | `(semaine, syndrome)` |
| `indicateurs_epidemiques` | Z-score, statut IAS, statut z-score, classification finale | `(semaine, syndrome)` |
| `rapports_ars` | Rapports JSON hebdomadaires avec situation globale | `semaine` |

Toutes les insertions utilisent `ON CONFLICT DO UPDATE` pour garantir l'**idempotence** du pipeline.

### Pipeline DAG

```
verifier_connexions → init_base_donnees → collecter_donnees_sursaud → archiver_local
→ verifier_archive → inserer_donnees_postgres → evaluer_situation_epidemique
    ├── declencher_alerte_ars           (si URGENCE)
    ├── envoyer_bulletin_surveillance   (si ALERTE)
    └── confirmer_situation_normale     (si NORMAL)
        └── generer_rapport_hebdomadaire
```

## Décisions techniques

### Image Docker
- **Base** : `apache/airflow:2.8.0`
- **Choix** : Image officielle Airflow légère, compatible CeleryExecutor.

### Réseau Docker
- Un réseau Docker Compose partagé entre tous les services. La connexion PostgreSQL depuis Airflow utilise le hostname `postgres-ars` (nom du service) sur le port interne `5432`.

### Bind mount pour les données
- Le dossier `./data/ars` est monté en bind mount dans les containers Airflow sur `/data/ars`, permettant :
  - La persistance des données entre redémarrages des containers.
  - L'accès direct aux fichiers depuis la machine hôte pour inspection et debug.

### Collecte des données IAS®
- **Source** : API data.gouv.fr (CSV IAS® OpenHealth).
- Le script `collecte_ias.py` télécharge les CSV, filtre sur la région Occitanie (`Loc_Reg76`), et agrège les données quotidiennes en valeurs hebdomadaires.
- Les données couvrent la saison épidémique (~octobre à avril). En dehors de cette période, les valeurs sont `null`.

### Calcul des indicateurs
- **Statut IAS** : comparaison de la valeur IAS aux seuils `seuil_min_saison` et `seuil_max_saison` du dataset.
- **Z-score** : calculé par rapport à l'historique des 5 saisons précédentes (même semaine ISO).
- **Classification finale** : le niveau le plus sévère entre statut IAS et statut z-score est retenu (`NORMAL` < `ALERTE` < `URGENCE`).

### Catchup et start_date
- `catchup=True` avec `start_date=2024-10-01` : le scheduler rattrape toutes les semaines depuis le début de la saison grippale 2024.

### Idempotence
- Le fichier SQL d'initialisation utilise `CREATE TABLE IF NOT EXISTS`, `ON CONFLICT DO NOTHING`, `CREATE OR REPLACE FUNCTION` et `DROP TRIGGER IF EXISTS` pour être rejouable sans erreur.
- Toutes les insertions de données utilisent `ON CONFLICT DO UPDATE`.

### Branching
- `BranchPythonOperator` pour `evaluer_situation_epidemique` : oriente le flux vers l'une des 3 branches (alerte, bulletin, normal) selon les indicateurs de la semaine.
- `generer_rapport_hebdomadaire` s'exécute avec `trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS` pour être atteint quelle que soit la branche.

## Difficultés rencontrées et solutions

| Difficulté | Solution |
|------------|----------|
| `PermissionError` sur `/data/ars/raw/` dans le container | Passage en bind mount avec `chmod -R 777 ./data/ars` sur l'hôte |
| `CREATE TRIGGER` échoue si le trigger existe déjà (pas de `IF NOT EXISTS` en PostgreSQL) | Ajout de `DROP TRIGGER IF EXISTS` avant chaque `CREATE TRIGGER` |
| Le scheduler ne recrée pas les DAG runs après modification du `start_date` | Suppression des anciens DAG runs en base, restart du scheduler, puis unpause du DAG |