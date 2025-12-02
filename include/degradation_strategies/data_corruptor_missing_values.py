# data_corruptor_missing_values.py
import pandas as pd
import numpy as np

def introduce_missing_values(df: pd.DataFrame, scenario: str) -> pd.DataFrame:
    """
    Introduce missing values in high-impact features based on corruption level.
    
    Args:
        df: Original German Credit dataset
        scenario: "light", "medium", or "severe"
        
    Returns:
        Corrupted DataFrame with missing values
    """
    df_corrupted = df.copy()
    
    # Define exact number of missing values based on corruption level
    if scenario == "light":
        missing_counts = {
            'checking_account_status': 20,
            'credit_history': 30,
            'credit_amount': 20,
            'duration_in_month': 20
        }
    elif scenario == "medium":
        missing_counts = {
            'checking_account_status': 40,
            'credit_history': 50,
            'credit_amount': 40,
            'duration_in_month': 40,
            'purpose': 40,
            'age_in_years': 50
        }
    else:  # severe
        missing_counts = {
            'checking_account_status': 20,
            'credit_history': 30,
            'credit_amount': 20,
            'duration_in_month': 80,
            'purpose': 50,
            'age_in_years': 50,
            'savings_account_bonds': 60,
            'foreign_worker': 50
        }
    
    for column, count in missing_counts.items():
        if column in df_corrupted.columns:
            # Randomly select 'count' indices to set as NaN
            if count < len(df_corrupted):
                indices = np.random.choice(len(df_corrupted), size=count, replace=False)
                df_corrupted.loc[indices, column] = np.nan
    
    return df_corrupted

