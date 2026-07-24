-- =====================================================================
-- Schéma PostgreSQL V3 — architecture de production churn CANAL+
-- CDC §4.7 (stockage), §4.8 (indexation), §8.3 (mitigation des risques)
--
-- Principes retenus :
--   - churn_data en APPEND-ONLY : chaque évènement Kafka (signup, usage,
--     paiement, résiliation) devient une nouvelle ligne. Le "dernier état"
--     d'un client est calculé par une vue (DISTINCT ON), jamais par UPDATE.
--   - churn_date arrive par RÉÉMISSION : à la résiliation, le producer
--     envoie un nouvel évènement avec Churn='Yes' et churn_date renseignée.
--   - actual_churn (vérité terrain) est calculé À LA VOLÉE par une vue
--     qui croise churn_model avec le dernier état de churn_data.
--     Aucun job périodique à maintenir : toujours frais, cohérent avec
--     l'objectif "tout en temps réel".
-- =====================================================================

-- ---------------------------------------------------------------
-- 1) CHURN_DATA — flux Kafka persistant (append-only)
--    23 variables : les 21 du dataset + SignupDate + ChurnDate
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS churn_data (
    id                  BIGSERIAL PRIMARY KEY,
    customer_id         VARCHAR(32)  NOT NULL,

    gender               VARCHAR(10),
    senior_citizen       SMALLINT,
    partner              VARCHAR(3),
    dependents           VARCHAR(3),
    tenure               INTEGER,
    phone_service        VARCHAR(3),
    multiple_lines       VARCHAR(20),
    internet_service     VARCHAR(20),
    online_security      VARCHAR(20),
    online_backup        VARCHAR(20),
    device_protection    VARCHAR(20),
    tech_support         VARCHAR(20),
    streaming_tv         VARCHAR(20),
    streaming_movies     VARCHAR(20),
    contract             VARCHAR(20),
    paperless_billing    VARCHAR(3),
    payment_method       VARCHAR(40),
    monthly_charges      NUMERIC(8,2),
    total_charges        NUMERIC(10,2),
    churn                VARCHAR(3),         -- 'Yes' / 'No'

    signup_date          DATE,               -- variable 22
    churn_date           DATE,               -- variable 23, NULL tant qu'actif

    -- Métadonnées de traçabilité du flux Kafka -> Spark -> PostgreSQL
    event_type           VARCHAR(20)  NOT NULL DEFAULT 'abonnement',  -- abonnement | usage | paiement | resiliation
    event_ts             TIMESTAMPTZ  NOT NULL,      -- horodatage de l'évènement Kafka
    ingested_ts           TIMESTAMPTZ  NOT NULL DEFAULT now(),
    source                VARCHAR(20)  NOT NULL DEFAULT 'kafka_simulator'
);

CREATE INDEX IF NOT EXISTS idx_churn_data_customer   ON churn_data (customer_id);
CREATE INDEX IF NOT EXISTS idx_churn_data_ingested   ON churn_data (ingested_ts DESC);
CREATE INDEX IF NOT EXISTS idx_churn_data_signup     ON churn_data (signup_date);
CREATE INDEX IF NOT EXISTS idx_churn_data_churndate  ON churn_data (churn_date) WHERE churn_date IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_churn_data_contract   ON churn_data (contract);
CREATE INDEX IF NOT EXISTS idx_churn_data_customer_ingested ON churn_data (customer_id, ingested_ts DESC);

-- Vue pivot : dernier état connu de chaque client (déduplique le flux append-only)
CREATE OR REPLACE VIEW v_churn_data_dernier_etat AS
SELECT DISTINCT ON (customer_id) *
FROM churn_data
ORDER BY customer_id, ingested_ts DESC;

-- ---------------------------------------------------------------
-- 2) CHURN_MODEL — table de scoring (append-only également)
--    Une ligne par prédiction ; la vérité terrain n'y est PAS stockée
--    en dur, elle est jointe en direct via une vue (cf. §4).
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS churn_model (
    id                    BIGSERIAL PRIMARY KEY,
    customer_id           VARCHAR(32)  NOT NULL,

    churn_score            NUMERIC(5,4) NOT NULL CHECK (churn_score BETWEEN 0 AND 1),
    churn_predicted         SMALLINT     NOT NULL,   -- 0/1 selon seuil (0,50 par défaut)
    seuil_applique          NUMERIC(4,3) NOT NULL DEFAULT 0.50,

    model_name              VARCHAR(64)  NOT NULL DEFAULT 'churn_model_canalplus',
    model_version            VARCHAR(16)  NOT NULL DEFAULT '1',

    event_ts                TIMESTAMPTZ  NOT NULL,
    predicted_ts             TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_churn_model_customer     ON churn_model (customer_id);
CREATE INDEX IF NOT EXISTS idx_churn_model_score        ON churn_model (churn_score DESC);
CREATE INDEX IF NOT EXISTS idx_churn_model_predicted_ts ON churn_model (predicted_ts DESC);
CREATE INDEX IF NOT EXISTS idx_churn_model_customer_ts  ON churn_model (customer_id, predicted_ts DESC);

-- Dernier score connu par client (lecture rapide, remplace scores_clients)
CREATE OR REPLACE VIEW v_dernier_score AS
SELECT DISTINCT ON (customer_id)
       customer_id, churn_score, churn_predicted, seuil_applique,
       model_version, predicted_ts,
       CASE WHEN churn_score >= 0.70 THEN 'ELEVE'
            WHEN churn_score >= 0.50 THEN 'MOYEN'
            ELSE 'FAIBLE' END AS niveau_risque
FROM churn_model
ORDER BY customer_id, predicted_ts DESC;

-- ---------------------------------------------------------------
-- 3) VUE CROISÉE — pont entre les deux mondes
--    Jointure du dernier état client + dernière prédiction.
--    Base de toute l'analyse prescriptive/prédictive (pages 3-5).
-- ---------------------------------------------------------------
CREATE OR REPLACE VIEW v_analyse_complete AS
SELECT
    d.customer_id, d.gender, d.senior_citizen, d.partner, d.dependents,
    d.tenure, d.contract, d.payment_method, d.internet_service,
    d.online_security, d.tech_support, d.monthly_charges, d.total_charges,
    d.churn AS churn_observe, d.signup_date, d.churn_date,
    CASE WHEN d.churn_date IS NOT NULL THEN (d.churn_date - d.signup_date)
         ELSE (CURRENT_DATE - d.signup_date) END AS duree_reelle_jours,
    s.churn_score, s.churn_predicted, s.niveau_risque, s.model_version, s.predicted_ts
FROM v_churn_data_dernier_etat d
LEFT JOIN v_dernier_score s ON s.customer_id = d.customer_id;

-- ---------------------------------------------------------------
-- 4) ACTUAL VS PREDICTED — calculé à la volée (aucun job à maintenir)
--    Reproduit le pattern "Actual / Predicted / Prediction Probability"
--    en direct sur les prédictions live.
-- ---------------------------------------------------------------
CREATE OR REPLACE VIEW v_actuals_vs_predictions AS
SELECT
    model_version,
    count(*)                                                          AS clients_evalues,
    count(*) FILTER (WHERE churn_observe = 'Yes')                     AS actual_churns,
    count(*) FILTER (WHERE churn_predicted = 1)                       AS predicted_churns,
    round(avg(CASE WHEN (churn_observe='Yes')::int = churn_predicted
                   THEN 1.0 ELSE 0 END), 4)                           AS accuracy_live,
    round(avg(churn_score), 4)                                        AS score_moyen
FROM v_analyse_complete
WHERE churn_score IS NOT NULL
GROUP BY model_version;

-- Top clients à risque, enrichi (remplace v_clients_risque)
CREATE OR REPLACE VIEW v_clients_risque AS
SELECT customer_id, churn_score, niveau_risque, contract, tenure,
       monthly_charges AS mrr, signup_date, predicted_ts
FROM v_analyse_complete
WHERE churn_predicted = 1
ORDER BY churn_score DESC;

-- KPIs live 24h (dashboard temps réel, page 4)
CREATE OR REPLACE VIEW v_kpis_live AS
SELECT
    count(*)                                                    AS nb_predictions_24h,
    count(DISTINCT customer_id)                                 AS nb_clients_24h,
    round(avg(churn_score), 4)                                   AS score_moyen_24h,
    round(avg(churn_predicted::numeric), 4)                      AS taux_churn_predit_24h,
    count(*) FILTER (WHERE churn_score >= 0.70)                  AS nb_risque_eleve_24h,
    round(sum(monthly_charges) FILTER (WHERE churn_predicted=1),2) AS mrr_a_risque_24h,
    max(predicted_ts)                                             AS derniere_prediction_ts
FROM v_analyse_complete
WHERE predicted_ts >= now() - INTERVAL '24 hours';

-- ---------------------------------------------------------------
-- 5) VUES DESCRIPTIVES — pages 1-4 (analyse historique)
-- ---------------------------------------------------------------
CREATE OR REPLACE VIEW v_churn_par_contrat AS
SELECT contract, count(*) AS nb_clients,
       count(*) FILTER (WHERE churn='Yes') AS nb_churn,
       round(avg((churn='Yes')::int::numeric), 4) AS taux_churn
FROM v_churn_data_dernier_etat GROUP BY contract;

CREATE OR REPLACE VIEW v_churn_par_tenure AS
SELECT width_bucket(tenure, 0, 72, 12) AS tranche,
       min(tenure) AS borne_basse, max(tenure) AS borne_haute,
       count(*) AS nb_clients,
       round(avg((churn='Yes')::int::numeric), 4) AS taux_churn
FROM v_churn_data_dernier_etat GROUP BY tranche ORDER BY tranche;

CREATE OR REPLACE VIEW v_churn_par_services AS
SELECT
  (CASE WHEN online_security='Yes' THEN 1 ELSE 0 END
 + CASE WHEN online_backup='Yes' THEN 1 ELSE 0 END
 + CASE WHEN device_protection='Yes' THEN 1 ELSE 0 END
 + CASE WHEN tech_support='Yes' THEN 1 ELSE 0 END
 + CASE WHEN streaming_tv='Yes' THEN 1 ELSE 0 END
 + CASE WHEN streaming_movies='Yes' THEN 1 ELSE 0 END) AS nb_services,
  count(*) AS nb_clients,
  round(avg((churn='Yes')::int::numeric), 4) AS taux_churn
FROM v_churn_data_dernier_etat GROUP BY nb_services ORDER BY nb_services;

-- ---------------------------------------------------------------
-- 6) VUES TEMPORELLES — nouvelles, exploitent signup_date/churn_date
-- ---------------------------------------------------------------

-- Cohortes d'inscription : rétention par mois de signup
CREATE OR REPLACE VIEW v_cohortes_retention AS
SELECT
    date_trunc('month', signup_date)::date AS mois_cohorte,
    count(*)                                AS taille_cohorte,
    count(*) FILTER (WHERE churn='Yes')     AS churners,
    round(avg((churn='Yes')::int::numeric), 4) AS taux_churn_cohorte,
    round(avg(EXTRACT(DAY FROM (churn_date - signup_date)))
          FILTER (WHERE churn_date IS NOT NULL), 1) AS duree_moy_avant_churn_jours
FROM v_churn_data_dernier_etat
WHERE signup_date IS NOT NULL
GROUP BY mois_cohorte ORDER BY mois_cohorte;

-- Délai réel avant résiliation, par contrat (vélocité de churn)
CREATE OR REPLACE VIEW v_velocite_churn AS
SELECT contract,
       count(*)                                                       AS nb_churners,
       round(avg(churn_date - signup_date), 1)                        AS delai_moyen_jours,
       round(percentile_cont(0.5) WITHIN GROUP
             (ORDER BY (churn_date - signup_date)), 1)                AS delai_median_jours
FROM v_churn_data_dernier_etat
WHERE churn='Yes' AND churn_date IS NOT NULL AND signup_date IS NOT NULL
GROUP BY contract;

-- Tendance calendaire hebdomadaire (saisonnalité)
CREATE OR REPLACE VIEW v_tendance_churn_hebdo AS
SELECT date_trunc('week', churn_date)::date AS semaine,
       count(*)                              AS nb_churns,
       round(avg(monthly_charges), 2)        AS mrr_moyen_perdu
FROM v_churn_data_dernier_etat
WHERE churn='Yes' AND churn_date IS NOT NULL
GROUP BY semaine ORDER BY semaine;
