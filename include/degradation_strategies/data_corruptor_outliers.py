# data_corruptor_outliers.py
import pandas as pd
import numpy as np

def introduce_outliers(df: pd.DataFrame, corruption_level: str = "medium") -> pd.DataFrame:
    """
    Introduce outliers in numerical features that break value range expectations.
    
    Args:
        df: Original German Credit dataset
        corruption_level: "light", "medium", or "severe"
        
    Returns:
        Corrupted DataFrame with outliers
    """
    df_corrupted = df.copy()
    
    # outlier parameters based on corruption level
    if corruption_level == "light":
        outlier_probs = {
            'credit_amount': 0.02,
            'duration_in_month': 0.02,
            'age_in_years': 0.02
        }
        multipliers = {
            'credit_amount': (3, 6),  # min, max multiplier
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

    else:  # severe
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
        if column in df_corrupted.columns:
            mask = np.random.random(len(df_corrupted)) < prob
            n_outliers = mask.sum()
            
            if n_outliers > 0:
                min_mult, max_mult = multipliers[column]
                multipliers_vals = np.random.uniform(min_mult, max_mult, n_outliers)
                
                # For some outliers, multiply; for others, set extreme values
                for i, idx in enumerate(df_corrupted[mask].index):
                    if np.random.random()  < (1 - extreme_prob): 
                        df_corrupted.loc[idx, column] *= multipliers_vals[i]
                    else:  # 30% set extreme fixed value
                        if column == 'credit_amount':
                            df_corrupted.loc[idx, column] = np.random.choice([50000, 100000, 250000])
                        elif column == 'duration_in_month':
                            df_corrupted.loc[idx, column] = np.random.choice([120, 180, 240])
                        elif column == 'age_in_years':
                            df_corrupted.loc[idx, column] = np.random.choice([100, 120, 150])
                        elif column == 'installment':
                            df_corrupted.loc[idx, column] *= np.random.uniform(4, 6)
                        elif column == 'residence':
                            df_corrupted.loc[idx, column] = np.random.choice([15, 20, 25])
    
    return df_corrupted