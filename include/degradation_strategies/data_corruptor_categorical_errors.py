# data_corruptor_categorical_errors.py
import pandas as pd
import numpy as np

def introduce_categorical_errors(df: pd.DataFrame, corruption_level: str = "medium") -> pd.DataFrame:
    """
    Introduce categorical value errors and invalid categories.
    
    Args:
        df: Original German Credit dataset
        corruption_level: "light", "medium", or "severe"
        
    Returns:
        Corrupted DataFrame with categorical errors
    """
    df_corrupted = df.copy()
    
    # invalid values for each categorical column
    invalid_values = {
        'checking_account_status': ["X99", "INVALID", "A99", ""],
        'credit_history': ["X99", "ERROR", "A99", "UNKNOWN"],
        'purpose': ["X99", "INVALID", "A99", "OTHER"],
        'savings_account_bonds': ["X99", "ERROR", "A99", ""],
        'foreign_worker': ["X99", "INVALID", "A99"],
        'property': ["X99", "ERROR", "A125", "A126"],
        'status_n_sex': ["X99", "INVALID", "A95", "A96"]
    }
    
    # corruption probabilities
    if corruption_level == "light":
        probabilities = {
            'checking_account_status': 0.04,
            'credit_history': 0.03,
            'purpose': 0.05
        }
    elif corruption_level == "medium":
        probabilities = {
            'checking_account_status': 0.10,
            'credit_history': 0.08,
            'purpose': 0.12,
            'savings_account_bonds': 0.06,
            'foreign_worker': 0.05
        }
    else:  # severe
        probabilities = {
            'checking_account_status': 0.31,
            'credit_history': 0.25,
            'purpose': 0.23,
            'savings_account_bonds': 0.24,
            'foreign_worker': 0.24,
            'property': 0.28,
            'status_n_sex': 0.28
        }
    
    for column, prob in probabilities.items():
        if column in df_corrupted.columns and column in invalid_values:
            mask = np.random.random(len(df_corrupted)) < prob
            n_errors = mask.sum()
            
            if n_errors > 0:
                invalid_choices = invalid_values[column]
                for idx in df_corrupted[mask].index:
                    df_corrupted.loc[idx, column] = np.random.choice(invalid_choices)
    
    return df_corrupted