-- =============================================================
--  SCHÉMA EN ÉTOILE — Version compatible MySQL (sans IF EXISTS)
-- =============================================================
USE jobs_db;

-- -------------------------------------------------------------
-- DIMENSIONS
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_source (
    source_id   INT AUTO_INCREMENT PRIMARY KEY,
    nom         VARCHAR(50)  NOT NULL,
    url_base    VARCHAR(200),
    pays        VARCHAR(50),
    type_source VARCHAR(50),
    UNIQUE KEY uq_source (nom)
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id       INT AUTO_INCREMENT PRIMARY KEY,
    date_complete DATE NOT NULL,
    jour          INT,
    mois          INT,
    mois_nom      VARCHAR(20),
    trimestre     INT,
    annee         INT,
    semaine       INT,
    jour_semaine  VARCHAR(20),
    UNIQUE KEY uq_date (date_complete)
);

CREATE TABLE IF NOT EXISTS dim_localisation (
    localisation_id INT AUTO_INCREMENT PRIMARY KEY,
    ville           VARCHAR(100),
    region          VARCHAR(100),
    pays            VARCHAR(50),
    teletravail     VARCHAR(30),
    UNIQUE KEY uq_loc (ville, pays, teletravail)
);

CREATE TABLE IF NOT EXISTS dim_contrat (
    contrat_id        INT AUTO_INCREMENT PRIMARY KEY,
    type_contrat      VARCHAR(50),
    niveau_experience VARCHAR(80),
    niveau_etude      VARCHAR(80),
    salaire_min       INT,
    salaire_max       INT,
    devise            VARCHAR(10) DEFAULT 'MAD'
);

CREATE TABLE IF NOT EXISTS dim_competence (
    competence_id INT AUTO_INCREMENT PRIMARY KEY,
    nom           VARCHAR(100) NOT NULL,
    categorie     VARCHAR(50),
    sous_categorie VARCHAR(50),
    UNIQUE KEY uq_comp (nom)
);

CREATE TABLE IF NOT EXISTS dim_offre_detail (
    offre_id    BIGINT PRIMARY KEY,
    titre       VARCHAR(250),
    entreprise  VARCHAR(150),
    description TEXT,
    missions    TEXT,
    profil      TEXT,
    url         VARCHAR(500),
    hash_offre  VARCHAR(64) UNIQUE
);

CREATE TABLE IF NOT EXISTS fact_offres (
    offre_id         BIGINT AUTO_INCREMENT PRIMARY KEY,
    source_id        INT,
    date_scraped_id  INT,
    date_publie_id   INT,
    localisation_id  INT,
    contrat_id       INT,
    nb_competences   INT     DEFAULT 0,
    est_active       BOOLEAN DEFAULT TRUE,
    score_pertinence FLOAT   DEFAULT 0.0,
    inserted_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (source_id)       REFERENCES dim_source(source_id),
    FOREIGN KEY (date_scraped_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (date_publie_id)  REFERENCES dim_date(date_id),
    FOREIGN KEY (localisation_id) REFERENCES dim_localisation(localisation_id),
    FOREIGN KEY (contrat_id)      REFERENCES dim_contrat(contrat_id)
);

CREATE TABLE IF NOT EXISTS fact_offre_competence (
    offre_id      BIGINT,
    competence_id INT,
    type_comp     VARCHAR(20) DEFAULT 'normalisee',
    PRIMARY KEY (offre_id, competence_id),
    FOREIGN KEY (offre_id)      REFERENCES fact_offres(offre_id),
    FOREIGN KEY (competence_id) REFERENCES dim_competence(competence_id)
);

-- -------------------------------------------------------------
-- Procédure pour créer un index s'il n'existe pas
-- -------------------------------------------------------------
DROP PROCEDURE IF EXISTS CreateIndexIfNotExists;

DELIMITER //
CREATE PROCEDURE CreateIndexIfNotExists(
    IN p_table_name VARCHAR(64),
    IN p_index_name VARCHAR(64),
    IN p_column_list VARCHAR(200)
)
BEGIN
    DECLARE index_exists INT DEFAULT 0;

    SELECT COUNT(*) INTO index_exists
    FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = p_table_name
      AND INDEX_NAME = p_index_name;

    IF index_exists = 0 THEN
        SET @sql = CONCAT('CREATE INDEX ', p_index_name, ' ON ', p_table_name, ' (', p_column_list, ')');
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;
END //
DELIMITER ;
--------------------------table user ------------------------------------

CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    preferences JSON,               -- exemple: {"competences":["python","sql"],"ville":"Casablanca","teletravail":true}
    notifications_enabled BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table pour l'historique des recherches
CREATE TABLE IF NOT EXISTS user_search_history (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    search_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    competences JSON,
    ville VARCHAR(100),
    teletravail BOOLEAN,
    type_contrat VARCHAR(50),
    niveau_experience VARCHAR(50),
    nb_results INT,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

-- Table pour l'historique des CV uploadés (optionnel)
CREATE TABLE IF NOT EXISTS user_cv_uploads (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    filename VARCHAR(255),
    extracted_skills JSON,
    FOREIGN KEY (user_id) REFERENCES users(id)
);
-- -------------------------------------------------------------
-- Création des index (uniquement s'ils n'existent pas)
-- -------------------------------------------------------------
CALL CreateIndexIfNotExists('fact_offres', 'idx_fact_source', 'source_id');
CALL CreateIndexIfNotExists('fact_offres', 'idx_fact_date', 'date_scraped_id');
CALL CreateIndexIfNotExists('fact_offres', 'idx_fact_loc', 'localisation_id');
CALL CreateIndexIfNotExists('fact_offres', 'idx_fact_contrat', 'contrat_id');
CALL CreateIndexIfNotExists('fact_offre_competence', 'idx_pont_offre', 'offre_id');
CALL CreateIndexIfNotExists('fact_offre_competence', 'idx_pont_comp', 'competence_id');
CALL CreateIndexIfNotExists('dim_competence', 'idx_comp_categorie', 'categorie');

-- Nettoyage de la procédure
DROP PROCEDURE IF EXISTS CreateIndexIfNotExists;

-- -------------------------------------------------------------
-- DONNÉES DE BASE dans dim_source
-- -------------------------------------------------------------
INSERT IGNORE INTO dim_source (nom, url_base, pays, type_source) VALUES
    ('rekrute',  'https://www.rekrute.com',  'Maroc',  'jobboard'),
    ('indeed',   'https://ma.indeed.com',    'Maroc',  'jobboard'),
    ('linkedin', 'https://www.linkedin.com', 'Global', 'reseau_social'),
    ('adzuna',   'https://www.adzuna.fr',    'France', 'jobboard'),
    ('remotive', 'https://remotive.com',     'Remote', 'jobboard');