from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
import sys
import os
import shutil
import json
from airflow.providers.postgres.hooks.postgres import PostgresHook
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
    "catchup": False,
}

def collecter_donnees_ias(**context) -> str:
    """Télécharge les CSV IAS® et retourne le chemin du fichier JSON
    créé."""
    semaine = (
        f"{context['execution_date'].year}-"
        f"S{context['execution_date'].isocalendar()[1]:02d}"
    )
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
    """Organise le fichier brut dans la structure d'archivage partitionnée."""
    semaine = (
        f"{context['execution_date'].year}-"
        f"S{context['execution_date'].isocalendar()[1]:02d}"
    )
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
    """Vérifie que le fichier d'archive existe et n'est pas vide."""
    semaine = (
        f"{context['execution_date'].year}-"
        f"S{context['execution_date'].isocalendar()[1]:02d}"
    )
    annee = semaine.split("-")[0]
    num_sem = semaine.split("-")[1]

    chemin = f"/data/ars/raw/{annee}/{num_sem}/sursaud_{semaine}.json"

    if not os.path.exists(chemin):
        raise FileNotFoundError(f"Archive manquante : {chemin}")

    taille = os.path.getsize(chemin)
    if taille == 0:
        raise ValueError(f"Archive vide : {chemin}")

    # Vérifier que c'est du JSON valide
    with open(chemin, "r", encoding="utf-8") as f:
        data = json.load(f)

    assert "syndromes" in data, "Clé 'syndromes' manquante"

    print(f"ARCHIVE_VALIDE:{chemin} ({taille} octets)")
    return True

def inserer_donnees_postgres(**context) -> None:
    """
    Insère les données hebdomadaires et les indicateurs dans
    PostgreSQL.
    Utilise ON CONFLICT DO UPDATE pour garantir l'idempotence.
    """
    semaine: str = context["templates_dict"]["semaine"]
    annee, num_sem = semaine.split("-")

    with open(f"/data/ars/raw/{annee}/{num_sem}/sursaud_{semaine}.json") as f:
        donnees_brutes = json.load(f)

    with open(f"/data/ars/indicateurs/indicateurs_{semaine}.json") as f:
        indicateurs = json.load(f)

    hook = PostgresHook(postgres_conn_id="postgres_ars")

    sql_donnees = """
        INSERT INTO donnees_hebdomadaires
            (code_dept, semaine, syndrome, nb_passages_urg,
             nb_consultations, nb_total_passages, taux_recours, source)
        VALUES
            (%(code_dept)s, %(semaine)s, %(syndrome)s, %(nb_passages_urg)s,
             %(nb_consultations)s, %(nb_total_passages)s, %(taux_recours)s, %(source)s)
        ON CONFLICT (code_dept, semaine, syndrome, source)
        DO UPDATE SET
            nb_passages_urg = EXCLUDED.nb_passages_urg,
            nb_consultations = EXCLUDED.nb_consultations,
            nb_total_passages = EXCLUDED.nb_total_passages,
            taux_recours = EXCLUDED.taux_recours,
            updated_at = CURRENT_TIMESTAMP;
    """

    sql_indicateurs = """
        INSERT INTO indicateurs_epidemiques
            (code_dept, semaine, syndrome, taux_incidence, z_score,
             r0_estime, nb_annees_reference, statut)
        VALUES
            (%(code_dept)s, %(semaine)s, %(syndrome)s, %(taux_incidence)s,
             %(z_score)s, %(r0_estime)s, %(nb_annees_reference)s, %(statut)s)
        ON CONFLICT (code_dept, semaine, syndrome)
        DO UPDATE SET
            taux_incidence = EXCLUDED.taux_incidence,
            z_score = EXCLUDED.z_score,
            r0_estime = EXCLUDED.r0_estime,
            nb_annees_reference = EXCLUDED.nb_annees_reference,
            statut = EXCLUDED.statut,
            updated_at = CURRENT_TIMESTAMP;
    """

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            for enreg in donnees_brutes.get("donnees", []):
                cur.execute(sql_donnees, {
                    "code_dept": enreg.get("code_dept"),
                    "semaine": enreg.get("semaine"),
                    "syndrome": enreg.get("syndrome"),
                    "nb_passages_urg": enreg.get("nb_passages_urg", 0),
                    "nb_consultations": enreg.get("nb_consultations", 0),
                    "nb_total_passages": enreg.get("nb_total_passages", 0),
                    "taux_recours": enreg.get("taux_recours"),
                    "source": enreg.get("source", "COMBINE"),
                })
            for indicateur in indicateurs:
                cur.execute(sql_indicateurs, indicateur)
            conn.commit()

    logging.info(
        f"{len(indicateurs)} indicateurs insérés/mis à jour pour la semaine {semaine}"
    )

def evaluer_situation_epidemique(**context) -> str:
    """
    Lit les indicateurs de la semaine depuis PostgreSQL et détermine le chemin
    d'exécution selon la situation épidémique la plus sévère observée.

    Returns: task_id de la branche à exécuter
    """
    semaine: str = context["templates_dict"]["semaine"]
    hook = PostgresHook(postgres_conn_id="postgres_ars")

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT statut,
                       COUNT(DISTINCT code_dept) AS nb_depts,
                       ARRAY_AGG(DISTINCT code_dept) AS depts
                FROM indicateurs_epidemiques
                WHERE semaine = %s
                GROUP BY statut
            """, (semaine,))
            resultats = {row[0]: {"nb": row[1], "depts": row[2]} for row in cur.fetchall()}

    nb_urgence = resultats.get("URGENCE", {}).get("nb", 0)
    nb_alerte = resultats.get("ALERTE", {}).get("nb", 0)
    depts_urgence = resultats.get("URGENCE", {}).get("depts", [])
    depts_alerte = resultats.get("ALERTE", {}).get("depts", [])

    context["task_instance"].xcom_push(key="nb_urgence", value=nb_urgence)
    context["task_instance"].xcom_push(key="nb_alerte", value=nb_alerte)
    context["task_instance"].xcom_push(key="depts_urgence", value=depts_urgence)
    context["task_instance"].xcom_push(key="depts_alerte", value=depts_alerte)

    logger.info(f"Semaine {semaine}: {nb_urgence} depts URGENCE, {nb_alerte} depts ALERTE")

    if nb_urgence > 0:
        return "declencher_alerte_ars"
    elif nb_alerte > 0:
        return "envoyer_bulletin_surveillance"
    else:
        return "confirmer_situation_normale"

def declencher_alerte_ars(**context) -> None:
    depts = context["task_instance"].xcom_pull(
        task_ids="evaluer_situation_epidemique", key="depts_urgence"
    )
    logger.critical(f"ALERTE ARS DÉCLENCHÉE — Départements en URGENCE : {depts}")

def envoyer_bulletin_surveillance(**context) -> None:
    depts = context["task_instance"].xcom_pull(
        task_ids="evaluer_situation_epidemique", key="depts_alerte"
    )
    logger.warning(f"Bulletin de surveillance envoyé — Départements en ALERTE : {depts}")

def confirmer_situation_normale(**context) -> None:
    logger.info("Situation épidémiologique normale en Occitanie — aucune action requise")

def verifier_connexions():
    conn_pg = BaseHook.get_connection("postgres_ars")
    depts = Variable.get("departements_occitanie", deserialize_json=True)
    print(f"PostgreSQL : {conn_pg.host}:{conn_pg.port}/{conn_pg.schema}")
    print(f"Départements configurés : {len(depts)}")

with DAG(
    dag_id="ars_epidemio_dag",
    default_args=default_args,
    description="Pipeline surveillance épidémiologique ARS Occitanie",
    schedule_interval="0 6 * * 1", # Tous les lundis à 6h UTC
    start_date=datetime(2024, 1, 1),
    catchup=True,
    max_active_runs=1,
    tags=["sante-publique", "epidemio", "docker-compose"],
) as dag:
    # Depuis un pythonOperator, verifier que les connexions et variables sont bien configurées
    verifier_connexions_task = PythonOperator(
        task_id="verifier_connexions",
        python_callable=verifier_connexions
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
        templates_dict={
            "semaine": "{{ execution_date.year }}-S{{ '%02d' % execution_date.isocalendar()[1] }}",
        },
        provide_context=True,
    )

    evaluer = BranchPythonOperator(
        task_id="evaluer_situation_epidemique",
        python_callable=evaluer_situation_epidemique,
        templates_dict={
            "semaine": "{{ execution_date.year }}-S{{ '%02d' % execution_date.isocalendar()[1] }}",
        },
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

    (
        verifier_connexions_task
        >> init_base_donnees
        >> collecter_sursaud
        >> archiver
        >> verifier
        >> inserer_postgres
        >> evaluer
        >> [alerte_ars, bulletin, normale]
    )