# data_corruptor_conjoined_corruption.py
import pandas as pd
import numpy as np
from degradation_strategies.data_corruptor_missing_values import introduce_missing_values
from degradation_strategies.data_corruptor_outliers import introduce_outliers
from degradation_strategies.data_corruptor_categorical_errors import introduce_categorical_errors
from degradation_strategies.data_corruptor_label_errors import introduce_label_errors
from degradation_strategies.data_corruptor_relationship_violations import introduce_relationship_violations

def create_conjoined_corruption(df: pd.DataFrame, corruption_level: str = "medium") -> pd.DataFrame:
    """
    Apply multiple corruption types to simulate most "real-world" data quality issues.
    
    Args:
        df: Original German Credit dataset
        corruption_level: "light", "medium", or "severe"
        
    Returns:
        Comprehensive corrupted DataFrame
    """
    
    # define which corruptions should be combined
    #df_corrupted = introduce_missing_values(df, corruption_level)
    #df_corrupted = introduce_outliers(df, corruption_level)
    df_corrupted = introduce_categorical_errors(df, corruption_level)
    #df_corrupted = introduce_label_errors(df_corrupted, corruption_level)
    #df_corrupted = introduce_relationship_violations(df_corrupted, corruption_level)
    
    return df_corrupted