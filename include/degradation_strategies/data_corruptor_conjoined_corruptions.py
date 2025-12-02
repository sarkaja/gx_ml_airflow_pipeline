# data_corruptor_conjoined_corruption.py
import pandas as pd
import numpy as np
from degradation_strategies.data_corruptor_missing_values import introduce_missing_values
from degradation_strategies.data_corruptor_outliers import introduce_outliers
from degradation_strategies.data_corruptor_categorical_errors import introduce_categorical_errors
from degradation_strategies.data_corruptor_label_errors import introduce_label_errors
from degradation_strategies.data_corruptor_relationship_violations import introduce_relationship_violations

def create_conjoined_corruption(df: pd.DataFrame, scenario: str) -> pd.DataFrame:
    """
    Apply multiple corruption types to simulate most "real-world" data quality issues.
    
    Args:
        df: Original German Credit dataset
        scenario: "light", "medium", or "severe"
        
    Returns:
        Comprehensive corrupted DataFrame
    """
    
    # define which corruptions should be combined
    df_corrupted = introduce_missing_values(df, scenario)
    df_corrupted = introduce_outliers(df_corrupted, scenario)
    df_corrupted = introduce_categorical_errors(df_corrupted, scenario)
    #df_corrupted = introduce_label_errors(df_corrupted, scenario)
    df_corrupted = introduce_relationship_violations(df_corrupted, scenario)
    
    return df_corrupted