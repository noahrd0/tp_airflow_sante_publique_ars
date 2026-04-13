-- ==========================================================
-- BASE DE DONNÉES : ars_epidemio
-- Schéma pour la surveillance épidémiologique ARS Occitanie
-- ==========================================================

-- Table des syndromes surveillés
CREATE TABLE IF NOT EXISTS syndromes (
    id SERIAL PRIMARY KEY,
    code VARCHAR(20) UNIQUE NOT NULL,
    libelle VARCHAR(100) NOT NULL,
    description TEXT,
    duree_infectieuse_jours INTEGER DEFAULT 5,
    actif BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Table des départements (référentiel géographique)
CREATE TABLE IF NOT EXISTS departements (
    id SERIAL PRIMARY KEY,
    code_dept VARCHAR(3) UNIQUE NOT NULL,
    nom VARCHAR(100) NOT NULL,
    code_region VARCHAR(3) NOT NULL,
    nom_region VARCHAR(100) NOT NULL,
    population INTEGER NOT NULL,
    superficie_km2 FLOAT,
    chef_lieu VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_departements_region
    ON departements (code_region);

-- Table des données hebdomadaires agrégées (IAS® Occitanie)
-- Une ligne par (semaine, syndrome) — agrégation hebdomadaire des données quotidiennes IAS
CREATE TABLE IF NOT EXISTS donnees_hebdomadaires (
    id SERIAL PRIMARY KEY,
    semaine VARCHAR(8) NOT NULL,                          -- format YYYY-SXX
    syndrome VARCHAR(20) NOT NULL REFERENCES syndromes(code),
    valeur_ias FLOAT NOT NULL,                            -- moyenne IAS Occitanie (Loc_Reg76) sur la semaine
    seuil_min_saison FLOAT,                               -- MIN_Saison du dataset (seuil épidémique bas)
    seuil_max_saison FLOAT,                               -- MAX_Saison du dataset (seuil pic épidémique)
    nb_jours_donnees INTEGER DEFAULT 0,                   -- nombre de jours disponibles pour l'agrégation
    annee INTEGER GENERATED ALWAYS AS (CAST(LEFT(semaine, 4) AS INTEGER)) STORED,
    numero_semaine INTEGER GENERATED ALWAYS AS (CAST(RIGHT(semaine, 2) AS INTEGER)) STORED,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_donnees_hbd UNIQUE (semaine, syndrome)
);

CREATE INDEX IF NOT EXISTS idx_donnees_hbd_semaine
    ON donnees_hebdomadaires (semaine);
CREATE INDEX IF NOT EXISTS idx_donnees_hbd_syndrome_annee
    ON donnees_hebdomadaires (syndrome, annee, numero_semaine);

-- Table des indicateurs épidémiques calculés (niveau régional Occitanie)
CREATE TABLE IF NOT EXISTS indicateurs_epidemiques (
    id SERIAL PRIMARY KEY,
    semaine VARCHAR(8) NOT NULL,
    syndrome VARCHAR(20) NOT NULL REFERENCES syndromes(code),
    valeur_ias FLOAT,                                     -- valeur IAS hebdomadaire Occitanie
    z_score FLOAT,                                        -- par rapport aux N-1..N-5 mêmes semaines
    r0_estime FLOAT,                                      -- reproduction number estimé (modèle SIR simplifié)
    nb_saisons_reference INTEGER,                         -- nombre de saisons historiques utilisées pour le z-score
    statut VARCHAR(10) CHECK (statut IN ('NORMAL', 'ALERTE', 'URGENCE')),
    statut_ias VARCHAR(10) CHECK (statut_ias IN ('NORMAL', 'ALERTE', 'URGENCE')),
    statut_zscore VARCHAR(10) CHECK (statut_zscore IN ('NORMAL', 'ALERTE', 'URGENCE')),
    commentaire TEXT,
    calcule_le TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_indicateurs UNIQUE (semaine, syndrome)
);

CREATE INDEX IF NOT EXISTS idx_indicateurs_statut
    ON indicateurs_epidemiques (statut, semaine);
CREATE INDEX IF NOT EXISTS idx_indicateurs_semaine
    ON indicateurs_epidemiques (semaine);

-- Table des rapports ARS générés
CREATE TABLE IF NOT EXISTS rapports_ars (
    id SERIAL PRIMARY KEY,
    semaine VARCHAR(8) NOT NULL,
    situation_globale VARCHAR(10) CHECK (situation_globale IN ('NORMAL', 'ALERTE', 'URGENCE')),
    nb_depts_alerte INTEGER DEFAULT 0,
    nb_depts_urgence INTEGER DEFAULT 0,
    rapport_json JSONB,                                   -- contenu complet du rapport
    chemin_local VARCHAR(500),                            -- chemin dans le volume Docker
    genere_le TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    genere_par VARCHAR(100) DEFAULT 'ars_epidemio_dag',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_rapport_semaine UNIQUE (semaine)
);

-- ==========================================================
-- DONNÉES DE RÉFÉRENCE
-- ==========================================================

-- Insertion des syndromes surveillés
INSERT INTO syndromes (code, libelle, description, duree_infectieuse_jours) VALUES
    ('GRIPPE',    'Grippe',                 'Syndrome grippal confirmé ou probable',            5),
    ('GEA',       'Gastro-entérite aiguë',  'Diarrhées et/ou vomissements aigus',               3),
    ('SG',        'Syndrome grippal',       'Fièvre + toux ou myalgies sans confirmation',       5),
    ('BRONCHIO',  'Bronchiolite',           'Bronchiolite du nourrisson (< 2 ans)',              7),
    ('COVID19',   'Covid-19',               'Syndrome Covid-19 confirmé ou probable',            7)
ON CONFLICT (code) DO NOTHING;

-- Insertion des 13 départements d'Occitanie
INSERT INTO departements (code_dept, nom, code_region, nom_region, population, chef_lieu) VALUES
    ('09', 'Ariège',                '76', 'Occitanie', 153153,  'Foix'),
    ('11', 'Aude',                  '76', 'Occitanie', 375438,  'Carcassonne'),
    ('12', 'Aveyron',               '76', 'Occitanie', 279594,  'Rodez'),
    ('30', 'Gard',                  '76', 'Occitanie', 750294,  'Nîmes'),
    ('31', 'Haute-Garonne',         '76', 'Occitanie', 1432680, 'Toulouse'),
    ('32', 'Gers',                  '76', 'Occitanie', 191283,  'Auch'),
    ('34', 'Hérault',               '76', 'Occitanie', 1175623, 'Montpellier'),
    ('46', 'Lot',                   '76', 'Occitanie', 174208,  'Cahors'),
    ('48', 'Lozère',                '76', 'Occitanie', 76601,   'Mende'),
    ('65', 'Hautes-Pyrénées',       '76', 'Occitanie', 228342,  'Tarbes'),
    ('66', 'Pyrénées-Orientales',   '76', 'Occitanie', 479979,  'Perpignan'),
    ('81', 'Tarn',                  '76', 'Occitanie', 389521,  'Albi'),
    ('82', 'Tarn-et-Garonne',       '76', 'Occitanie', 261100,  'Montauban')
ON CONFLICT (code_dept) DO NOTHING;

-- Trigger pour mettre à jour updated_at automatiquement
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_donnees_hbd_updated_at ON donnees_hebdomadaires;
CREATE TRIGGER update_donnees_hbd_updated_at
    BEFORE UPDATE ON donnees_hebdomadaires
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_indicateurs_updated_at ON indicateurs_epidemiques;
CREATE TRIGGER update_indicateurs_updated_at
    BEFORE UPDATE ON indicateurs_epidemiques
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
