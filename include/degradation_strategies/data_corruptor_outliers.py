# data_corruptor_outliers.py
import pandas as pd
import numpy as np


def introduce_outliers(
    df: pd.DataFrame,
    corruption_level: str,
    random_state: int | None = None
) -> pd.DataFrame:
    """
    Introduce outliers into numerical features that break expected value ranges.

    Args:
        df: Original dataset to be corrupted.
        corruption_level: One of "light", "medium", or "severe". Higher levels
            affect more columns and use higher probabilities and more extreme
            multipliers for outliers.
        random_state: Optional seed for reproducibility.

    Returns:
        A corrupted DataFrame with injected outliers according to the selected
        corruption level.
    """
    df_corrupted = df.copy()
    rng = np.random.default_rng(random_state)
    n_rows = len(df_corrupted)

    # outlier parameters based on corruption level
    if corruption_level == "light":
        outlier_probs = {
            'credit_amount': 0.02,
            'duration_in_month': 0.02,
            'age_in_years': 0.02
        }
        multipliers = {
            'credit_amount': (3, 6),   # min, max multiplier
            'duration_in_month': (3, 6),
            'age_in_years': (1.5, 2.5)
        }
        extreme_prob = 0.20

    elif corruption_level == "medium":
        outlier_probs = {
            'credit_amount': 0.04,
            'duration_in_month': 0.04,
            'age_in_years': 0.03,
            'installment': 0.03
        }
        multipliers = {
            'credit_amount': (5, 8),
            'duration_in_month': (5, 8),
            'age_in_years': (2, 3),
            'installment': (2, 3)
        }
        extreme_prob = 0.30

    else:  # "severe"
        outlier_probs = {
            'credit_amount': 0.07,
            'duration_in_month': 0.06,
            'age_in_years': 0.07,
            'installment': 0.05,
            'residence': 0.05
        }
        multipliers = {
            'credit_amount': (7, 12),
            'duration_in_month': (6, 10),
            'age_in_years': (2.5, 4),
            'installment': (3, 6),
            'residence': (3, 6)
        }
        extreme_prob = 0.40

    for column, prob in outlier_probs.items():
        if column not in df_corrupted.columns or n_rows == 0:
            continue

        # mask of rows where outliers will be introduced for this column
        mask = rng.random(n_rows) < prob
        if not mask.any():
            continue

        idx = df_corrupted.index[mask]
        current_vals = df_corrupted.loc[idx, column].to_numpy(dtype=float)

        # multipliers for outliers
        min_mult, max_mult = multipliers[column]
        multipliers_vals = rng.uniform(min_mult, max_mult, size=len(idx))

        # decide which outliers will be "extreme" vs. "just multiplied"
        extreme_mask = rng.random(len(idx)) < extreme_prob

        new_vals = current_vals.copy()

        # 1) regular outliers – only scaling by a multiplier
        normal_mask = ~extreme_mask
        if normal_mask.any():
            new_vals[normal_mask] = current_vals[normal_mask] * multipliers_vals[normal_mask]

        # 2) extreme outliers – fixed values / special logic
        if extreme_mask.any():
            if column == 'credit_amount':
                extreme_vals = rng.choice([50000, 100000, 250000], size=extreme_mask.sum())
            elif column == 'duration_in_month':
                extreme_vals = rng.choice([120, 180, 240], size=extreme_mask.sum())
            elif column == 'age_in_years':
                extreme_vals = rng.choice([100, 120, 150], size=extreme_mask.sum())
            elif column == 'installment':
                # for installment, extremes = multiples between 4–6
                extreme_multipliers = rng.uniform(4, 6, size=extreme_mask.sum())
                extreme_vals = current_vals[extreme_mask] * extreme_multipliers
            elif column == 'residence':
                extreme_vals = rng.choice([15, 20, 25], size=extreme_mask.sum())
            else:
                # fallback – just a larger multiplier
                extreme_vals = current_vals[extreme_mask] * multipliers_vals[extreme_mask]

            new_vals[extreme_mask] = extreme_vals

        # write back into the DataFrame
        df_corrupted.loc[idx, column] = new_vals

    return df_corrupted
