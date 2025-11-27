# data_corruptor_relationship_violations.py
import pandas as pd
import numpy as np

def introduce_relationship_violations(df: pd.DataFrame, corruption_level: str = "medium") -> pd.DataFrame:
    """
    Introduce violations in data relationships and business logic.
    
    Args:
        df: Original German Credit dataset
        corruption_level: "light", "medium", or "severe"
        
    Returns:
        Corrupted DataFrame with relationship violations
    """
    df_corrupted = df.copy()
    
    if corruption_level == "light":
        prob = 0.02
    elif corruption_level == "medium":
        prob = 0.05
    else:  # more severe but still can be an outlier
        prob = 0.08
    
    # Violate: credit_amount should be >= duration_in_month
    mask = np.random.random(len(df_corrupted)) < prob
    n_violations = mask.sum()
    
    if n_violations > 0:
        # duration larger than credit amount (business logic violation)
        for idx in df_corrupted[mask].index:
            current_credit = df_corrupted.loc[idx, 'credit_amount']
            if current_credit > 0:
                df_corrupted.loc[idx, 'duration_in_month'] = current_credit * np.random.uniform(1.1, 2.0)
    
    return df_corrupted