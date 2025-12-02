# data_corruptor_categorical_errors.py
import pandas as pd
import numpy as np


def introduce_categorical_errors(
    df: pd.DataFrame,
    scenario: str,
    random_state: int | None = None
) -> pd.DataFrame:
    """
    Introduce categorical value errors and invalid categories.

    Args:
        df: Original dataset to be corrupted.
        scenario: Corruption scenario, one of "light", "medium", or "severe".
                  Higher severity introduces errors into more columns and with
                  a higher probability.
        random_state: Optional seed for reproducibility.

    Returns:
        A corrupted DataFrame with injected categorical errors and invalid
        category values according to the selected scenario.
    """
    df_corrupted = df.copy()
    rng = np.random.default_rng(random_state)
    n_rows = len(df_corrupted)

    # invalid values for each categorical column
    invalid_values = {
        'checking_account_status': ["X99", "INVALID", "A99", ""],
        'credit_history': ["X99", "ERROR", "A99", "OTHER"],
        'purpose': ["X99", "INVALID", "A99", "OTHER"],
        'savings_account_bonds': ["X99", "ERROR", "A99", ""],
        'foreign_worker': ["X99", "INVALID", "A99", "AER"],
        'property': ["X99", "ERROR", "A125", "A126", "A89"],
        'status_n_sex': ["X99", "INVALID", "A95", "A96", ""],
    }

    # base grouping of columns by "importance"
    base_cols = ["checking_account_status", "credit_history", "purpose"]
    medium_extra = ["savings_account_bonds", "foreign_worker"]
    severe_extra = ["property", "status_n_sex"]

    if scenario == "light":
        cols_to_corrupt = base_cols
        prob = 0.03
    elif scenario == "medium":
        cols_to_corrupt = base_cols + medium_extra
        prob = 0.05
    else:  # "severe"
        cols_to_corrupt = base_cols + medium_extra + severe_extra
        prob = 0.08

    for column in cols_to_corrupt:
        if column not in df_corrupted.columns:
            continue
        if column not in invalid_values:
            continue
        if n_rows == 0:
            continue

        # mask of rows where we introduce errors
        mask = rng.random(n_rows) < prob
        if not mask.any():
            continue

        idx = df_corrupted.index[mask]
        choices = invalid_values[column]
        # random invalid values for all selected rows
        sampled_invalids = rng.choice(choices, size=len(idx), replace=True)
        df_corrupted.loc[idx, column] = sampled_invalids

    return df_corrupted
