-- ============================================================
-- DATA QUALITY RESULTS
-- Stores the outcome of Great Expectations validation runs.
-- ============================================================

CREATE TABLE IF NOT EXISTS public.dq_results (
    id SERIAL PRIMARY KEY,
    dataset_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    expectation_suite_name TEXT NOT NULL,

    total_expectations INT NOT NULL,
    successful_expectations INT NOT NULL,
    failed_expectations INT NOT NULL,
    success_rate DECIMAL(5,2) NOT NULL,
    overall_success BOOLEAN NOT NULL,

    completeness_ok BOOLEAN,
    consistency_ok BOOLEAN,

    failed_columns TEXT[],
    critical_failures INT NOT NULL,
    warning_failures INT NOT NULL,

    key_columns_validation JSONB,

    validation_duration_seconds DECIMAL(10,3),
    records_processed INT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dq_results_dataset_id ON public.dq_results(dataset_id);
CREATE INDEX IF NOT EXISTS idx_dq_results_run_id ON public.dq_results(run_id);
CREATE INDEX IF NOT EXISTS idx_dq_results_created_at ON public.dq_results(created_at);
CREATE INDEX IF NOT EXISTS idx_dq_results_success ON public.dq_results(overall_success);


-- ============================================================
-- DATA QUALITY ALERTS
-- Stores alert messages about detected DQ issues.
-- ============================================================

CREATE TABLE IF NOT EXISTS public.dq_alerts (
    id SERIAL PRIMARY KEY,
    dataset_id TEXT,
    run_id TEXT,
    severity TEXT,
    message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);


-- ============================================================
-- MACHINE LEARNING RESULTS
-- Stores trained model performance and CV metrics.
-- ============================================================

CREATE TABLE IF NOT EXISTS public.ml_results (
    id SERIAL PRIMARY KEY,

    dataset_id TEXT NOT NULL,
    model_name TEXT NOT NULL,
    run_id TEXT NOT NULL,

    accuracy DECIMAL(5,4) NOT NULL,
    precision DECIMAL(5,4) NOT NULL,
    recall DECIMAL(5,4) NOT NULL,
    f1_score DECIMAL(5,4) NOT NULL,
    roc_auc DECIMAL(5,4) NOT NULL,

    cv_accuracy_mean DECIMAL(5,4) NOT NULL,
    cv_accuracy_std DECIMAL(5,4) NOT NULL,
    cv_precision_mean DECIMAL(5,4) NOT NULL,
    cv_precision_std DECIMAL(5,4) NOT NULL,
    cv_recall_mean DECIMAL(5,4) NOT NULL,
    cv_recall_std DECIMAL(5,4) NOT NULL,
    cv_f1_mean DECIMAL(5,4) NOT NULL,
    cv_f1_std DECIMAL(5,4) NOT NULL,
    cv_roc_auc_mean DECIMAL(5,4) NOT NULL,
    cv_roc_auc_std DECIMAL(5,4) NOT NULL,

    confusion_matrix JSONB,
    classification_report TEXT,
    is_best_model BOOLEAN DEFAULT FALSE,

    test_size DECIMAL(3,2) NOT NULL,
    cv_folds INT NOT NULL,
    random_state INT NOT NULL,

    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ml_results_dataset_id ON public.ml_results(dataset_id);
CREATE INDEX IF NOT EXISTS idx_ml_results_model_name ON public.ml_results(model_name);
CREATE INDEX IF NOT EXISTS idx_ml_results_run_id ON public.ml_results(run_id);
CREATE INDEX IF NOT EXISTS idx_ml_results_created_at ON public.ml_results(created_at);
CREATE INDEX IF NOT EXISTS idx_ml_results_best_model ON public.ml_results(is_best_model);


-- View listing best-performing models across datasets
CREATE OR REPLACE VIEW public.v_best_models AS
SELECT 
    dataset_id,
    model_name,
    accuracy,
    roc_auc,
    created_at
FROM public.ml_results 
WHERE is_best_model = TRUE
ORDER BY dataset_id, created_at DESC;


-- ============================================================
-- RAW DATA TABLE
-- Stores original (uncorrupted) German Credit records.
-- ============================================================

CREATE TABLE IF NOT EXISTS public.raw_german_credit_data (
    id SERIAL PRIMARY KEY,
    dataset_id TEXT NOT NULL,
    data_source TEXT,

    checking_account_status        TEXT,
    duration_in_month              INT,
    credit_history                 TEXT,
    purpose                        TEXT,
    credit_amount                  INT,
    savings_account_bonds          TEXT,
    employment                     TEXT,
    installment                    INT,
    status_n_sex                   TEXT,
    other_debtors_guarantors       TEXT,
    residence                      INT,
    property                       TEXT,
    age_in_years                   INT,
    other_installment_plans        TEXT,
    housing                        TEXT,
    existing_credits_no            INT,
    job                            TEXT,
    liability_responsibles         INT,
    telephone                      TEXT,
    foreign_worker                 TEXT,
    category                       INT,

    loaded_at TIMESTAMP DEFAULT NOW(),
    run_id TEXT
);


-- ============================================================
-- CORRUPTED DATA TABLE
-- Stores deliberately corrupted data used for testing DQ.
-- ============================================================

CREATE TABLE IF NOT EXISTS public.corrupted_german_credit_data (
    id SERIAL PRIMARY KEY,
    dataset_id TEXT,
    corruption_type TEXT,
    corruption_scenario TEXT,
    original_row_id INT,

    checking_account_status        TEXT,
    duration_in_month              INT,
    credit_history                 TEXT,
    purpose                        TEXT,
    credit_amount                  INT,
    savings_account_bonds          TEXT,
    employment                     TEXT,
    installment                    INT,
    status_n_sex                   TEXT,
    other_debtors_guarantors       TEXT,
    residence                      INT,
    property                       TEXT,
    age_in_years                   INT,
    other_installment_plans        TEXT,
    housing                        TEXT,
    existing_credits_no            INT,
    job                            TEXT,
    liability_responsibles         INT,
    telephone                      TEXT,
    foreign_worker                 TEXT,
    category                       INT,

    loaded_at TIMESTAMP DEFAULT NOW(),
    run_id TEXT
);


-- ============================================================
-- REMEDIATED DATA TABLE
-- Stores cleaned/improved versions of corrupted datasets.
-- ============================================================

CREATE TABLE IF NOT EXISTS public.remediated_german_credit_data (
    id SERIAL PRIMARY KEY,
    dataset_id TEXT,
    corruption_type TEXT,
    corruption_scenario TEXT,
    original_row_id INT,

    checking_account_status        TEXT,
    duration_in_month              INT,
    credit_history                 TEXT,
    purpose                        TEXT,
    credit_amount                  INT,
    savings_account_bonds          TEXT,
    employment                     TEXT,
    installment                    INT,
    status_n_sex                   TEXT,
    other_debtors_guarantors       TEXT,
    residence                      INT,
    property                       TEXT,
    age_in_years                   INT,
    other_installment_plans        TEXT,
    housing                        TEXT,
    existing_credits_no            INT,
    job                            TEXT,
    liability_responsibles         INT,
    telephone                      TEXT,
    foreign_worker                 TEXT,
    category                       INT,

    loaded_at TIMESTAMP DEFAULT NOW(),
    run_id TEXT
);


-- ============================================================
-- DATASET METADATA TABLE
-- Stores metadata about dataset versions and lifecycle status.
-- ============================================================

CREATE TABLE IF NOT EXISTS public.dataset_metadata (
    id SERIAL PRIMARY KEY,
    dataset_name TEXT UNIQUE NOT NULL,
    dataset_type TEXT,
    description TEXT,
    row_count INT,
    column_count INT,
    data_quality_score DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    is_active BOOLEAN DEFAULT TRUE,
    current_version TEXT
);

CREATE INDEX IF NOT EXISTS idx_dataset_metadata_name ON public.dataset_metadata(dataset_name);
CREATE INDEX IF NOT EXISTS idx_dataset_metadata_type ON public.dataset_metadata(dataset_type);
CREATE INDEX IF NOT EXISTS idx_dataset_metadata_active ON public.dataset_metadata(is_active);


-- ============================================================
-- DATASET OVERVIEW VIEW
-- Shows active datasets and summary metrics.
-- ============================================================

CREATE OR REPLACE VIEW public.v_dataset_overview AS
SELECT 
    dataset_name,
    dataset_type,
    row_count,
    column_count,
    data_quality_score,
    created_at,
    updated_at,
    is_active
FROM public.dataset_metadata
WHERE is_active = TRUE
ORDER BY dataset_type, created_at DESC;


-- ============================================================
-- DATA LINEAGE VIEW
-- Tracks dataset flow across RAW → CORRUPTED → REMEDIATED.
-- ============================================================

CREATE OR REPLACE VIEW public.v_data_lineage AS
SELECT 
    'raw' AS data_type,
    dataset_id,
    COUNT(*) AS record_count,
    MIN(loaded_at) AS first_load,
    MAX(loaded_at) AS last_load,
    run_id
FROM public.raw_german_credit_data 
GROUP BY dataset_id, run_id

UNION ALL

SELECT 
    'corrupted' AS data_type,
    dataset_id,
    COUNT(*) AS record_count,
    MIN(loaded_at) AS first_load,
    MAX(loaded_at) AS last_load,
    run_id
FROM public.corrupted_german_credit_data 
GROUP BY dataset_id, run_id

UNION ALL

SELECT 
    'remediated' AS data_type,
    dataset_id,
    COUNT(*) AS record_count,
    MIN(loaded_at) AS first_load,
    MAX(loaded_at) AS last_load,
    run_id
FROM public.remediated_german_credit_data 
GROUP BY dataset_id, run_id;
