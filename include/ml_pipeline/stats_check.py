import numpy as np
import pandas as pd
from scipy.stats import entropy
import logging

logger = logging.getLogger("airflow.task")
from scripts.get_environment_config import ProjectConfig


def get_original_dataset() -> pd.DataFrame:
    """
    Load the original (clean) dataset from the RAW table.

    Returns:
        DataFrame representing the clean, uncorrupted data.
    """
    from sql.postgres_manager import PostgresManager

    cfg = ProjectConfig()
    table_name = cfg.table_names["raw"]

    db_manager = PostgresManager()
    df_before = db_manager.load_clean_data(
        data_source="original_csv",
        table_name=table_name,
    )
    return df_before


def calculate_psi(expected, actual, buckets=10) -> float:
    """
    Compute the Population Stability Index (PSI) between two distributions.

    Args:
        expected: Baseline distribution.
        actual: New/current distribution.
        buckets: Number of bins for numeric PSI.

    Returns:
        PSI value measuring distribution drift.
    """
    try:
        # Numeric PSI
        expected_perc = pd.cut(expected, bins=buckets).value_counts(normalize=True, sort=False)
        actual_perc = pd.cut(actual, bins=buckets).value_counts(normalize=True, sort=False)
    except Exception:
        # Categorical PSI fallback
        expected_perc = expected.value_counts(normalize=True)
        actual_perc = actual.value_counts(normalize=True)

    psi = np.sum(
        (expected_perc - actual_perc)
        * np.log((expected_perc + 1e-8) / (actual_perc + 1e-8))
    )
    return psi


def evaluate_remediation_bias(df_after: pd.DataFrame) -> dict:
    """
    Assess distribution bias introduced by data remediation.

    Evaluates:
        - Missing value reduction
        - Category dominance drift
        - KL divergence
        - Entropy changes
        - Numeric percentile drift
        - PSI drift

    Args:
        df_after: DataFrame after remediation is applied.

    Returns:
        Dictionary summarizing drift and bias metrics per column.
    """
    CREDIT_COLUMNS = [
        "checking_account_status",
        "duration_in_month",
        "credit_history",
        "purpose",
        "credit_amount",
        "savings_account_bonds",
        "employment",
        "installment",
        "status_n_sex",
        "other_debtors_guarantors",
        "residence",
        "property",
        "age_in_years",
        "other_installment_plans",
        "housing",
        "existing_credits_no",
        "job",
        "liability_responsibles",
        "telephone",
        "foreign_worker",
    ]

    # Load reference/original data
    df_before = get_original_dataset()
    df_before = df_before[CREDIT_COLUMNS]
    df_after = df_after[CREDIT_COLUMNS]

    results = {}

    for col in df_before.columns:
        if col not in df_after.columns:
            continue

        before = df_before[col]
        after = df_after[col]
        col_result = {}

        # ------- Missing Value Drift -------
        missing_before = before.isna().mean()
        missing_after = after.isna().mean()

        col_result["missing_before"] = missing_before
        col_result["missing_after"] = missing_after
        col_result["missing_reduction"] = missing_before - missing_after

        # ------- Categorical Drift -------
        if before.dtype == "object" or before.dtype.name == "category":
            before_dist = before.value_counts(normalize=True)
            after_dist = after.value_counts(normalize=True)

            dominant_before = before_dist.max() if not before_dist.empty else 0
            dominant_after = after_dist.max() if not after_dist.empty else 0

            col_result["dominant_growth"] = dominant_after - dominant_before

            # Align category indexes for KL divergence
            common_idx = sorted(set(before_dist.index).union(after_dist.index))
            b = before_dist.reindex(common_idx, fill_value=0)
            a = after_dist.reindex(common_idx, fill_value=0)

            col_result["kl_divergence"] = entropy(b + 1e-8, a + 1e-8)
            col_result["entropy_before"] = entropy(b + 1e-8)
            col_result["entropy_after"] = entropy(a + 1e-8)
            col_result["entropy_drop"] = col_result["entropy_after"] - col_result["entropy_before"]

            col_result["psi"] = calculate_psi(before, after)

        else:
            # ------- Numeric Drift -------
            before_num = pd.to_numeric(before, errors="coerce")
            after_num = pd.to_numeric(after, errors="coerce")

            # Percentile drift
            for p in [0.5, 0.9, 0.95, 0.99]:
                col_result[f"p{int(p*100)}_before"] = before_num.quantile(p)
                col_result[f"p{int(p*100)}_after"] = after_num.quantile(p)
                col_result[f"p{int(p*100)}_drift"] = (
                    col_result[f"p{int(p*100)}_after"]
                    - col_result[f"p{int(p*100)}_before"]
                )

            # KL divergence via histograms
            hist_before, _ = np.histogram(before_num.dropna(), bins=20, density=True)
            hist_after, _ = np.histogram(after_num.dropna(), bins=20, density=True)

            col_result["kl_divergence"] = entropy(hist_before + 1e-8, hist_after + 1e-8)
            col_result["entropy_before"] = entropy(hist_before + 1e-8)
            col_result["entropy_after"] = entropy(hist_after + 1e-8)
            col_result["entropy_drop"] = col_result["entropy_after"] - col_result["entropy_before"]
            col_result["psi"] = calculate_psi(before_num, after_num)

        results[col] = col_result

    logger.info(f"Remediation bias and drift report: {results}")
    return results
