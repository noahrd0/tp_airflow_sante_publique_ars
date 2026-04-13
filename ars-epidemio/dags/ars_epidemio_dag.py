from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule
import sys
import os
import shutil
import json
import logging

logger = logging.getLogger(__name__)

default_args = {
    "owner": "ars-occitanie",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


def _get_semaine(context):
    return (
        f"{context['execution_date'].year}-"
        f"S{context['execution_date'].isocalendar()[1]:02d}"
    )


def collecter_donnees_ias(**context) -> str:
    semaine = _get_semaine(context)
    archive_path = Variable.get("archive_base_path", default_var="/data/ars")
    output_dir = f"{archive_path}/raw"
    sys.path.insert(0, "/opt/airflow/scripts")

    from collecte_ias import (
        DATASETS_IAS, telecharger_csv_ias, filtrer_semaine,
        agreger_semaine, sauvegarder_donnees
    )

    resultats = {}
    for syndrome, url in DATASETS_IAS.items():
        rows_all = telecharger_csv_ias(url)
        rows_sem = filtrer_semaine(rows_all, semaine)
        resultats[syndrome] = agreger_semaine(rows_sem, syndrome, semaine)
    return sauvegarder_donnees(resultats, semaine, output_dir)


def archiver_local(**context) -> str:
    semaine = _get_semaine(context)
    annee = semaine.split("-")[0]
    num_sem = semaine.split("-")[1]

    chemin_source = context["task_instance"].xcom_pull(
        task_ids="collecter_donnees_sursaud"
    )

    archive_dir = f"/data/ars/raw/{annee}/{num_sem}"
    os.makedirs(archive_dir, exist_ok=True)
    chemin_dest = f"{archive_dir}/sursaud_{semaine}.json"
    shutil.copy2(chemin_source, chemin_dest)

    print(f"ARCHIVE_OK:{chemin_dest}")
    return chemin_dest


def verifier_archive(**context) -> bool:
    semaine = _get_semaine(context)
    annee = semaine.split("-")[0]
    num_sem = semaine.split("-")[1]

    chemin = f"/data/ars/raw/{annee}/{num_sem}/sursaud_{semaine}.json"

    if not os.path.exists(chemin):
        raise FileNotFoundError(f"Archive manquante : {chemin}")

    taille = os.path.getsize(chemin)
    if taille == 0:
        raise ValueError(f"Archive vide : {chemin}")

    with open(chemin, "r", encoding="utf-8") as f:
        data = json.load(f)

    assert "syndromes" in data, "Clé 'syndromes' manquante"
    print(f"ARCHIVE_VALIDE:{chemin} ({taille} octets)")
    return True


def inserer_donnees_postgres(**context) -> None:
    """Insère les données hebdomadaires dans PostgreSQL (idempotent)."""
    semaine = _get_semaine(context)
    annee = semaine.split("-")[0]
    num_sem = semaine.split("-")[1]

    chemin = f"/data/ars/raw/{annee}/{num_sem}/sursaud_{semaine}.json"
    with open(chemin, "r", encoding="utf-8") as f:
        donnees_brutes = json.load(f)

    hook = PostgresHook(postgres_conn_id="postgres_ars")

    sql_donnees = """
        INSERT INTO donnees_hebdomadaires
            (semaine, syndrome, valeur_ias, seuil_min_saison, seuil_max_saison, nb_jours_donnees)
        VALUES
            (%(semaine)s, %(syndrome)s, %(valeur_ias)s,
             %(seuil_min)s, %(seuil_max)s, %(nb_jours)s)
        ON CONFLICT (semaine, syndrome)
        DO UPDATE SET
            valeur_ias = EXCLUDED.valeur_ias,
            seuil_min_saison = EXCLUDED.seuil_min_saison,
            seuil_max_saison = EXCLUDED.seuil_max_saison,
            nb_jours_donnees = EXCLUDED.nb_jours_donnees,
            updated_at = CURRENT_TIMESTAMP;
    """

    sql_indicateurs = """
        INSERT INTO indicateurs_epidemiques
            (semaine, syndrome, valeur_ias, z_score, r0_estime,
             nb_saisons_reference, statut, statut_ias, statut_zscore, commentaire)
        VALUES
            (%(semaine)s, %(syndrome)s, %(valeur_ias)s, %(z_score)s,
             %(r0_estime)s, %(nb_saisons_reference)s, %(statut)s,
             %(statut_ias)s, %(statut_zscore)s, %(commentaire)s)
        ON CONFLICT (semaine, syndrome)
        DO UPDATE SET
            valeur_ias = EXCLUDED.valeur_ias,
            z_score = EXCLUDED.z_score,
            r0_estime = EXCLUDED.r0_estime,
            statut = EXCLUDED.statut,
            statut_ias = EXCLUDED.statut_ias,
            statut_zscore = EXCLUDED.statut_zscore,
            commentaire = EXCLUDED.commentaire,
            updated_at = CURRENT_TIMESTAMP;
    """

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            syndromes = donnees_brutes.get("syndromes", {})
            for syndrome_code, data in syndromes.items():
                valeur_ias = data.get("valeur_ias", 0) or 0

                # Insertion des données hebdomadaires
                cur.execute(sql_donnees, {
                    "semaine": semaine,
                    "syndrome": syndrome_code,
                    "valeur_ias": valeur_ias,
                    "seuil_min": data.get("seuil_min"),
                    "seuil_max": data.get("seuil_max"),
                    "nb_jours": data.get("nb_jours", 0),
                })

                # Calcul simple des indicateurs à partir des données IAS
                seuil_min = data.get("seuil_min")
                seuil_max = data.get("seuil_max")

                # Classification par seuils IAS
                if seuil_max and valeur_ias > seuil_max:
                    statut_ias = "URGENCE"
                elif seuil_min and valeur_ias > seuil_min:
                    statut_ias = "ALERTE"
                else:
                    statut_ias = "NORMAL"

                # Calcul z-score simplifié
                historique = data.get("historique", {})
                hist_vals = [v for v in historique.values() if v is not None]
                z_score = None
                statut_zscore = "NORMAL"
                if len(hist_vals) >= 2 and valeur_ias:
                    import statistics
                    mean_hist = statistics.mean(hist_vals)
                    std_hist = statistics.stdev(hist_vals)
                    if std_hist > 0:
                        z_score = round((valeur_ias - mean_hist) / std_hist, 3)
                        if z_score > 2.5:
                            statut_zscore = "URGENCE"
                        elif z_score > 1.5:
                            statut_zscore = "ALERTE"

                # Statut final
                statut = "NORMAL"
                if "URGENCE" in (statut_ias, statut_zscore):
                    statut = "URGENCE"
                elif "ALERTE" in (statut_ias, statut_zscore):
                    statut = "ALERTE"

                cur.execute(sql_indicateurs, {
                    "semaine": semaine,
                    "syndrome": syndrome_code,
                    "valeur_ias": valeur_ias,
                    "z_score": z_score,
                    "r0_estime": None,
                    "nb_saisons_reference": len(hist_vals),
                    "statut": statut,
                    "statut_ias": statut_ias,
                    "statut_zscore": statut_zscore,
                    "commentaire": f"IAS={statut_ias}, Z={statut_zscore}",
                })

            conn.commit()

    logger.info(f"{len(syndromes)} syndromes insérés pour {semaine}")


def evaluer_situation_epidemique(**context) -> str:
    semaine = _get_semaine(context)
    hook = PostgresHook(postgres_conn_id="postgres_ars")

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT statut, COUNT(*) AS nb
                FROM indicateurs_epidemiques
                WHERE semaine = %s
                GROUP BY statut
            """, (semaine,))
            resultats = {row[0]: row[1] for row in cur.fetchall()}

    nb_urgence = resultats.get("URGENCE", 0)
    nb_alerte = resultats.get("ALERTE", 0)

    context["task_instance"].xcom_push(key="nb_urgence", value=nb_urgence)
    context["task_instance"].xcom_push(key="nb_alerte", value=nb_alerte)

    logger.info(f"Semaine {semaine}: {nb_urgence} URGENCE, {nb_alerte} ALERTE")

    if nb_urgence > 0:
        return "declencher_alerte_ars"
    elif nb_alerte > 0:
        return "envoyer_bulletin_surveillance"
    else:
        return "confirmer_situation_normale"


def declencher_alerte_ars(**context) -> None:
    logger.critical("ALERTE ARS DÉCLENCHÉE")


def envoyer_bulletin_surveillance(**context) -> None:
    logger.warning("Bulletin de surveillance envoyé")


def confirmer_situation_normale(**context) -> None:
    logger.info("Situation épidémiologique normale — aucune action requise")


def generer_rapport_hebdomadaire(**context) -> None:
    """
    Génère le rapport hebdomadaire de surveillance épidémiologique.
    S'exécute quelle que soit la branche empruntée.
    """
    semaine: str = context["templates_dict"]["semaine"]
    hook = PostgresHook(postgres_conn_id="postgres_ars")

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT syndrome, valeur_ias, z_score, r0_estime,
                       statut, statut_ias, statut_zscore,
                       nb_saisons_reference, commentaire
                FROM indicateurs_epidemiques
                WHERE semaine = %s
                ORDER BY statut DESC, valeur_ias DESC
            """, (semaine,))
            indicateurs = cur.fetchall()

    statuts = [row[4] for row in indicateurs]
    if "URGENCE" in statuts:
        situation_globale = "URGENCE"
    elif "ALERTE" in statuts:
        situation_globale = "ALERTE"
    else:
        situation_globale = "NORMAL"

    syndromes_urgence = [row[0] for row in indicateurs if row[4] == "URGENCE"]
    syndromes_alerte = [row[0] for row in indicateurs if row[4] == "ALERTE"]

    recommandations_par_niveau = {
        "URGENCE": [
            "Activation du plan de réponse épidémique régional",
            "Renforcement des équipes de surveillance dans les services d'urgences",
            "Communication renforcée auprès des professionnels de santé libéraux",
            "Notification immédiate à Santé Publique France et au Ministère de la Santé",
        ],
        "ALERTE": [
            "Surveillance renforcée des indicateurs pour les 48h suivantes",
            "Envoi d'un bulletin de surveillance aux partenaires de santé",
            "Vérification des capacités d'accueil des services d'urgences",
        ],
        "NORMAL": [
            "Maintien de la surveillance standard",
            "Prochain point épidémiologique dans 7 jours",
        ],
    }

    rapport = {
        "semaine": semaine,
        "region": "Occitanie",
        "code_region": "76",
        "date_generation": datetime.utcnow().isoformat(),
        "situation_globale": situation_globale,
        "nb_syndromes_surveilles": len(indicateurs),
        "syndromes_en_urgence": syndromes_urgence,
        "syndromes_en_alerte": syndromes_alerte,
        "indicateurs": [
            {
                "syndrome": row[0],
                "valeur_ias": row[1],
                "z_score": row[2],
                "r0_estime": row[3],
                "statut": row[4],
                "statut_ias": row[5],
                "statut_zscore": row[6],
                "nb_saisons_reference": row[7],
                "commentaire": row[8],
            }
            for row in indicateurs
        ],
        "recommandations": recommandations_par_niveau[situation_globale],
        "genere_par": "ars_epidemio_dag v1.0",
        "pipeline_version": "2.8",
    }

    annee = semaine.split("-")[0]
    num_sem = semaine.split("-")[1]
    local_path = f"/data/ars/rapports/{annee}/{num_sem}/rapport_{semaine}.json"
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(rapport, f, ensure_ascii=False, indent=2)

    hook2 = PostgresHook(postgres_conn_id="postgres_ars")
    with hook2.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO rapports_ars
                    (semaine, situation_globale, nb_depts_alerte,
                     nb_depts_urgence, rapport_json, chemin_local)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (semaine) DO UPDATE SET
                    situation_globale = EXCLUDED.situation_globale,
                    nb_depts_alerte  = EXCLUDED.nb_depts_alerte,
                    nb_depts_urgence = EXCLUDED.nb_depts_urgence,
                    rapport_json     = EXCLUDED.rapport_json,
                    chemin_local     = EXCLUDED.chemin_local,
                    updated_at       = CURRENT_TIMESTAMP
            """, (
                semaine, situation_globale,
                len(syndromes_alerte), len(syndromes_urgence),
                json.dumps(rapport, ensure_ascii=False),
                local_path,
            ))
            conn.commit()

    logger.info(f"Rapport {semaine} généré — Statut : {situation_globale}")


def verifier_connexions():
    conn_pg = BaseHook.get_connection("postgres_ars")
    print(f"PostgreSQL : {conn_pg.host}:{conn_pg.port}/{conn_pg.schema}")


with DAG(
    dag_id="ars_epidemio_dag",
    default_args=default_args,
    description="Pipeline surveillance épidémiologique ARS Occitanie",
    schedule_interval="0 6 * * 1",
    start_date=datetime(2024, 10, 1),
    catchup=True,
    max_active_runs=1,
    tags=["sante-publique", "epidemio", "docker-compose"],
) as dag:
    verifier_connexions_task = PythonOperator(
        task_id="verifier_connexions",
        python_callable=verifier_connexions,
    )

    init_base_donnees = PostgresOperator(
        task_id="init_base_donnees",
        postgres_conn_id="postgres_ars",
        sql="sql/init_ars_epidemio.sql",
        autocommit=True,
    )

    collecter_sursaud = PythonOperator(
        task_id="collecter_donnees_sursaud",
        python_callable=collecter_donnees_ias,
        provide_context=True,
    )

    archiver = PythonOperator(
        task_id="archiver_local",
        python_callable=archiver_local,
        provide_context=True,
    )

    verifier = PythonOperator(
        task_id="verifier_archive",
        python_callable=verifier_archive,
        provide_context=True,
    )

    inserer_postgres = PythonOperator(
        task_id="inserer_donnees_postgres",
        python_callable=inserer_donnees_postgres,
        provide_context=True,
    )

    evaluer = BranchPythonOperator(
        task_id="evaluer_situation_epidemique",
        python_callable=evaluer_situation_epidemique,
        provide_context=True,
    )

    alerte_ars = PythonOperator(
        task_id="declencher_alerte_ars",
        python_callable=declencher_alerte_ars,
        provide_context=True,
    )

    bulletin = PythonOperator(
        task_id="envoyer_bulletin_surveillance",
        python_callable=envoyer_bulletin_surveillance,
        provide_context=True,
    )

    normale = PythonOperator(
        task_id="confirmer_situation_normale",
        python_callable=confirmer_situation_normale,
        provide_context=True,
    )

    generer_rapport = PythonOperator(
        task_id="generer_rapport_hebdomadaire",
        python_callable=generer_rapport_hebdomadaire,
        templates_dict={
            "semaine": "{{ execution_date.year }}-S{{ '%02d' % execution_date.isocalendar()[1] }}",
        },
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        provide_context=True,
    )

    (
        verifier_connexions_task
        >> init_base_donnees
        >> collecter_sursaud
        >> archiver
        >> verifier
        >> inserer_postgres
        >> evaluer
        >> [alerte_ars, bulletin, normale]
        >> generer_rapport
    )