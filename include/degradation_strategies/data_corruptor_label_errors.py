# data_corruptor_label_errors.py
import pandas as pd
import numpy as np

def introduce_label_errors(df: pd.DataFrame, corruption_level: str = "medium") -> pd.DataFrame:
    """
    Introduce errors in the target variable (label noise).
    Critical for ML performance evaluation.
    
    Args:
        df: Original German Credit dataset
        corruption_level: "light", "medium", or "severe"
        
    Returns:
        Corrupted DataFrame with label errors
    """
    df_corrupted = df.copy()
    
    # label corruption probabilities
    if corruption_level == "light":
        flip_prob = 0.02  # 2% of labels flipped
    elif corruption_level == "medium":
        flip_prob = 0.5  # 5% of labels flipped
    else:  # severe
        flip_prob = 0.08  # 8% of labels flipped
    
    if 'category' in df_corrupted.columns:
        mask = np.random.random(len(df_corrupted)) < flip_prob
        # flipping labels: 0 <-> 1
        df_corrupted.loc[mask, 'category'] = 1 - df_corrupted.loc[mask, 'category']  # 0->1, 1->0
    
    return df_corrupted