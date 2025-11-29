# data_quality_improver.py
import pandas as pd
import numpy as np
from sklearn.impute import SimpleImputer, KNNImputer
from sklearn.ensemble import IsolationForest
from scipy import stats
import warnings
warnings.filterwarnings('ignore')
import logging 

logger = logging.getLogger(__name__)

class DataQualityImprover:
    """
    Improves data quality using various cleaning techniques.
    """
    
    def __init__(self, validation_rules: dict):
        """
        Initialize the data quality improver.
        
        Args:
            validation_rules: Dictionary with validation rules from YAML
        """
        self.validation_rules = validation_rules
        self.imputation_strategies = {}
    
    def improve_data_quality(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Main method for improving data quality.
        
        Args:
            df: Input DataFrame with data quality issues
            
        Returns:
            Improved DataFrame with better data quality
        """
        df_improved = df.copy()
        
        # 1. fixing missing values
        df_improved = self._handle_missing_values(df_improved)
        
        # 2. correcting outliers
        df_improved = self._handle_outliers(df_improved)
        
        # 3. fixing categorical errors
        df_improved = self._handle_categorical_errors(df_improved)
        
        # 4. handling relationship violations
        df_improved = self._handle_relationship_violations(df_improved)
        
        return df_improved
    
    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle missing values using appropriate imputation strategies.
        
        Args:
            df: DataFrame with missing values
            
        Returns:
            DataFrame with imputed missing values
        """
        df_clean = df.copy()
        
        for column in df_clean.columns:
            if df_clean[column].isna().sum() > 0:
                
                # Numerical columns
                if pd.api.types.is_numeric_dtype(df_clean[column]):
                    # Use KNN imputation for important features
                    if column in ['credit_amount', 'duration_in_month', 'age_in_years']:
                        imputer = KNNImputer(n_neighbors=5)
                        df_clean[column] = imputer.fit_transform(df_clean[[column]])[:, 0]
                    else:
                        # Median for other numerical columns
                        median_val = df_clean[column].median()
                        df_clean[column].fillna(median_val, inplace=True)
                
                # Categorical columns
                else:
                    # Find most frequent value from validation rules
                    valid_values = self._get_valid_values_for_column(column)
                    if valid_values:
                        # Use mode but only from valid values
                        valid_data = df_clean[df_clean[column].isin(valid_values)][column]
                        if not valid_data.empty:
                            mode_val = valid_data.mode()
                            fill_value = mode_val[0] if not mode_val.empty else valid_values[0]
                        else:
                            fill_value = valid_values[0]
                    else:
                        # If no rules, use mode
                        mode_val = df_clean[column].mode()
                        fill_value = mode_val[0] if not mode_val.empty else 'UNKNOWN'
                    
                    df_clean[column].fillna(fill_value, inplace=True)
        
        return df_clean
    
    def _handle_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Detect and correct outliers in numerical columns.
        
        Args:
            df: DataFrame with potential outliers
            
        Returns:
            DataFrame with corrected outliers
        """
        df_clean = df.copy()
        
        # Numerical columns for outlier detection
        numeric_columns = df_clean.select_dtypes(include=[np.number]).columns
        
        for column in numeric_columns:
            if column in ['credit_amount', 'duration_in_month', 'age_in_years', 'installment', 'residence']:
                # Method 1: IQR method
                logger.info(f"column numerical  {column}")
                Q1 = df_clean[column].quantile(0.25)
                Q3 = df_clean[column].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                # Method 2: Z-score for extreme outliers
                #z_scores = np.abs(stats.zscore(df_clean[column].dropna()))
                
                # Combined outlier mask
                outlier_mask = ((df_clean[column] < lower_bound) | 
                              (df_clean[column] > upper_bound)) & df_clean[column].notna()
                
                if outlier_mask.sum() > 0:
                    # Winsorization - cap outliers at boundaries
                    df_clean.loc[df_clean[column] < lower_bound, column] = lower_bound
                    df_clean.loc[df_clean[column] > upper_bound, column] = upper_bound
        
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
        logger.info(f"{numeric_columns} numeric_columns ")
        if len(numeric_columns) > 0:
            iso_forest = IsolationForest(contamination=contamination, random_state=42)
            outliers = iso_forest.fit_predict(df_clean[numeric_columns])
            logger.info(f"{outliers} dsjk ")
            outlier_mask = outliers == -1
            if outlier_mask.sum() > 0:
                logger.info(f"{outlier_mask} outlier_mask ")
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
    

    def _handle_categorical_errors(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fix invalid categorical values based on validation rules.
        Args:
            df: DataFrame with categorical errors
        Returns:
            DataFrame with corrected categorical values
        """
        df_clean = df.copy()
        for column in df_clean.columns:
            if not pd.api.types.is_numeric_dtype(df_clean[column]):
                valid_values = self._get_valid_values_for_column(column)
                if valid_values:
                    # Find invalid values
                    invalid_mask = ~df_clean[column].isin(valid_values) & df_clean[column].notna()
                    if invalid_mask.sum() > 0:
                        # Replace invalid values with most frequent valid value
                        valid_data = df_clean[df_clean[column].isin(valid_values)][column]
                        if not valid_data.empty:
                            replacement = valid_data.mode()
                            replacement_value = replacement[0] if not replacement.empty else valid_values[0]
                        else:
                            replacement_value = valid_values[0]
                        df_clean.loc[invalid_mask, column] = replacement_value
        return df_clean


    def _handle_relationship_violations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fix business logic violations between columns.
        
        Args:
            df: DataFrame with relationship violations
            
        Returns:
            DataFrame with corrected relationships
        """
        df_clean = df.copy()
        
        # Fix: credit_amount must be >= duration_in_month
        if 'credit_amount' in df_clean.columns and 'duration_in_month' in df_clean.columns:
            violation_mask = df_clean['credit_amount'] < df_clean['duration_in_month']
            if violation_mask.sum() > 0:
                # Set credit_amount to reasonable multiple of duration
                df_clean.loc[violation_mask, 'credit_amount'] = (
                    df_clean.loc[violation_mask, 'duration_in_month'] * 
                    np.random.uniform(10, 50, violation_mask.sum())
                )
        
        # Fix: age_in_years must be reasonable
        if 'age_in_years' in df_clean.columns:
            age_mask = (df_clean['age_in_years'] < 18) | (df_clean['age_in_years'] > 100)
            if age_mask.sum() > 0:
                # set age to median from reasonable range
                reasonable_ages = df_clean[(df_clean['age_in_years'] >= 18) & 
                                         (df_clean['age_in_years'] <= 100)]['age_in_years']
                if not reasonable_ages.empty:
                    median_age = reasonable_ages.median()
                    df_clean.loc[age_mask, 'age_in_years'] = median_age
        
        return df_clean
    
    def _get_valid_values_for_column(self, column: str) -> list:
        """
        Get valid values for a column from validation rules.
        
        Args:
            column: Column name to get valid values for
            
        Returns:
            List of valid values or None if no restrictions
        """
        valid_values = []
        
        if 'expectations' in self.validation_rules:
            for expectation in self.validation_rules['expectations']:
                if expectation.get('kwargs', {}).get('column') == column:
                    if 'expect_column_values_to_be_in_set' in expectation['expectation_type']:
                        valid_values = expectation['kwargs'].get('value_set', [])
                    elif 'expect_column_values_to_be_between' in expectation['expectation_type']:
                        # For numerical columns return None - IQR should be used instaed
                        return None
        
        return valid_values