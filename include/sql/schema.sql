
CREATE TABLE IF NOT EXISTS public.dq_results (
    id SERIAL PRIMARY KEY,
    dataset_id TEXT NOT NULL,
    dq_run_id TEXT NOT NULL,
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
CREATE INDEX IF NOT EXISTS idx_dq_results_run_id ON public.dq_results(dq_run_id);
CREATE INDEX IF NOT EXISTS idx_dq_results_created_at ON public.dq_results(created_at);
CREATE INDEX IF NOT EXISTS idx_dq_results_success ON public.dq_results(overall_success);


CREATE TABLE IF NOT EXISTS public.dq_alerts (
    id SERIAL PRIMARY KEY,
    dataset_id TEXT,
    dq_run_id INT,
    severity TEXT,
    message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);


-- ML results table for storing model evaluation results
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
    cv_folds INTEGER NOT NULL,
    random_state INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ml_results_dataset_id ON public.ml_results(dataset_id);
CREATE INDEX IF NOT EXISTS idx_ml_results_model_name ON public.ml_results(model_name);
CREATE INDEX IF NOT EXISTS idx_ml_results_run_id ON public.ml_results(run_id);
CREATE INDEX IF NOT EXISTS idx_ml_results_created_at ON public.ml_results(created_at);
CREATE INDEX IF NOT EXISTS idx_ml_results_best_model ON public.ml_results(is_best_model);

-- View for best models per dataset
CREATE OR REPLACE VIEW public.v_best_models AS
SELECT 
    dataset_id,
    model_name,
    accuracy,
    roc_auc,
    created_at
FROM public.ml_results 
WHERE is_best_model = true
ORDER BY dataset_id, created_at DESC;
