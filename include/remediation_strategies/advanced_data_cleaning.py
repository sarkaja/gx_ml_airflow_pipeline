# advanced_data_cleaning.py
import pandas as pd
import numpy as np
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import LabelEncoder
from typing import Dict, List

class AdvancedDataCleaning:
    """
    Advanced techniques for improving data quality.
    """
    
    def __init__(self):
        self.label_encoders = {}
    
    def iterative_imputation(self, df: pd.DataFrame, numeric_columns: List[str]) -> pd.DataFrame:
        """
        Perform iterative imputation using MICE algorithm.
        
        Args:
            df: DataFrame with missing values
            numeric_columns: List of numerical column names
            
        Returns:
            DataFrame with imputed numerical values
        """
        df_clean = df.copy()
        
        # Only for numerical columns
        imputer = IterativeImputer(max_iter=10, random_state=42)
        df_clean[numeric_columns] = imputer.fit_transform(df_clean[numeric_columns])
        
        return df_clean
    
    def multivariate_outlier_detection(self, df: pd.DataFrame, contamination: float = 0.1) -> pd.DataFrame:
        """
        Detect outliers using Isolation Forest.
        
        Args:
            df: DataFrame to check for outliers
            contamination: Expected proportion of outliers
            
        Returns:
            DataFrame with corrected outliers
        """
        df_clean = df.copy()
        
        numeric_columns = df_clean.select_dtypes(include=[np.number]).columns
        if len(numeric_columns) > 0:
            iso_forest = IsolationForest(contamination=contamination, random_state=42)
            outliers = iso_forest.fit_predict(df_clean[numeric_columns])
            
            outlier_mask = outliers == -1
            if outlier_mask.sum() > 0:
                for column in numeric_columns:
                    Q1 = df_clean[column].quantile(0.25)
                    Q3 = df_clean[column].quantile(0.75)
                    IQR = Q3 - Q1
                    
                    # Winsorize outliers insteda of removing them - adjustable! 
                    lower_bound = Q1 - 1.5 * IQR
                    upper_bound = Q3 + 1.5 * IQR
                    
                    column_outliers = outlier_mask & (
                        (df_clean[column] < lower_bound) | 
                        (df_clean[column] > upper_bound)
                    )
                    
                    if column_outliers.sum() > 0:
                        df_clean.loc[df_clean[column] < lower_bound, column] = lower_bound
                        df_clean.loc[df_clean[column] > upper_bound, column] = upper_bound
        
        return df_clean
    
    def smart_categorical_imputation(self, df: pd.DataFrame, target_column: str = 'category') -> pd.DataFrame:
        """
        Smart imputation of categorical variables based on target variable.
        
        Args:
            df: DataFrame with categorical missing values
            target_column: Target variable column name
            
        Returns:
            DataFrame with imputed categorical values
        """
        df_clean = df.copy()
        
        categorical_columns = df_clean.select_dtypes(include=['object']).columns
        
        for column in categorical_columns:
            if df_clean[column].isna().sum() > 0 and target_column in df_clean.columns:
                # Find most probable value based on target variable
                temp_df = df_clean[df_clean[column].notna()]
                if not temp_df.empty:
                    # Probability of each category given target
                    prob_matrix = temp_df.groupby([column, target_column]).size().unstack(fill_value=0)
                    prob_matrix = prob_matrix.div(prob_matrix.sum(axis=1), axis=0)
                    
                    # For each missing value find best category
                    for idx in df_clean[df_clean[column].isna()].index:
                        target_val = df_clean.loc[idx, target_column]
                        if not pd.isna(target_val):
                            best_category = prob_matrix[target_val].idxmax()
                            df_clean.loc[idx, column] = best_category
                        else:
                            # If target also missing, use mode
                            mode_val = temp_df[column].mode()
                            df_clean.loc[idx, column] = mode_val[0] if not mode_val.empty else 'UNKNOWN'
        
        return df_clean