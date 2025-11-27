# data_corruptor_missing_values.py
import pandas as pd
import numpy as np
import random

def introduce_missing_values(df: pd.DataFrame, corruption_level: str = "medium") -> pd.DataFrame:
    """
    Introduce missing values in high-impact features based on corruption level.
    
    Args:
        df: Original German Credit dataset
        corruption_level: "light", "medium", or "severe"
        
    Returns:
        Corrupted DataFrame with missing values
    """
    df_corrupted = df.copy()
    
    # Define missing value probabilities based on corruption level
    if corruption_level == "light":
        probabilities = {
            'checking_account_status': 0.05,
            'credit_history': 0.03,
            'credit_amount': 0.02,
            'duration_in_month': 0.02
        }
    elif corruption_level == "medium":
        probabilities = {
            'checking_account_status': 0.15,
            'credit_history': 0.10,
            'credit_amount': 0.08,
            'duration_in_month': 0.08,
            'purpose': 0.10,
            'age_in_years': 0.05
        }
    else:  # severe
        probabilities = {
            'checking_account_status': 0.25,
            'credit_history': 0.20,
            'credit_amount': 0.15,
            'duration_in_month': 0.15,
            'purpose': 0.20,
            'age_in_years': 0.10,
            'savings_account_bonds': 0.15,
            'foreign_worker': 0.10
        }
    
    for column, prob in probabilities.items():
        if column in df_corrupted.columns:
            mask = np.random.random(len(df_corrupted)) < prob
            df_corrupted.loc[mask, column] = np.nan
    
    return df_corrupted