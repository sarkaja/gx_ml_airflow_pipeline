# unified_data_quality_improver.py
import pandas as pd
import numpy as np
from sklearn.impute import KNNImputer
from sklearn.ensemble import IsolationForest
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer
import warnings
from typing import Dict, List, Set
import logging

warnings.filterwarnings('ignore')
logger = logging.getLogger(__name__)

class UnifiedDataQualityImprover:
    """
    Unified data quality improver with conditional remediation based on failed expectations.
    """
    
    def __init__(self, validation_rules: dict):
        self.validation_rules = validation_rules
        self.label_encoders = {}
        self.failed_expectations = []
        
    def set_failed_expectations(self, failed_expectations: List[Dict]):
        """Set the failed expectations to determine which remediations to run."""
        self.failed_expectations = failed_expectations or []
        
    def get_failed_expectation_types(self) -> Set[str]:
        """Get unique types of failed expectations."""
        return {exp.get('type', '') for exp in self.failed_expectations}
    
    def get_failed_columns(self) -> Set[str]:
        """Get columns that have failed expectations."""
        return {exp.get('column', '') for exp in self.failed_expectations if exp.get('column')}
    
    def should_run_remediation(self, expectation_type: str, column: str = None) -> bool:
        """Check if remediation should run based on failed expectations."""
        for failed_exp in self.failed_expectations:
            if failed_exp.get('type') == expectation_type:
                if column is None or failed_exp.get('column') == column:
                    return True
        return False
    
    def improve_data_quality(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Main method for improving data quality - runs only needed remediations.
        
        Args:
            df: Input DataFrame with data quality issues
            
        Returns:
            Improved DataFrame with better data quality
        """
        if not self.failed_expectations:
            logger.info("No failed expectations - skipping remediation")
            return df.copy()
            
        df_improved = df.copy()
        failed_columns = self.get_failed_columns()
        failed_types = self.get_failed_expectation_types()
        
        logger.info(f"Running remediations for failed types: {failed_types}")
        logger.info(f"Running remediations for columns: {failed_columns}")
        
        # 1. Handle missing values for columns with null expectation failures
        if any('null' in exp_type for exp_type in failed_types):
            df_improved = self._handle_missing_values(df_improved, failed_columns)
        
        # 2. Handle outliers for columns with range expectation failures
        if any('between' in exp_type for exp_type in failed_types):
            df_improved = self._handle_outliers(df_improved, failed_columns)
        
        # 3. Handle categorical errors for columns with set expectation failures
        if any('in_set' in exp_type for exp_type in failed_types):
            df_improved = self._handle_categorical_errors(df_improved, failed_columns)
        
        # 4. Handle relationship violations
        df_improved = self._handle_relationship_violations(df_improved)
        
        # 5. Advanced techniques for complex cases
        df_improved = self._run_advanced_techniques(df_improved, failed_types, failed_columns)
        
        return df_improved
    
    def _handle_missing_values(self, df: pd.DataFrame, failed_columns: Set[str]) -> pd.DataFrame:
        """
        Remediate missing values only for columns that failed null-related expectations.

        Args:
            df: Input dataframe.
            failed_columns: Columns that failed null expectations.

        Returns:
            Dataframe with missing values handled selectively.
        """
        df_clean = df.copy()

        for column in failed_columns:
            if column not in df_clean.columns:
                continue

            if not self.should_run_remediation("expect_column_values_to_not_be_null", column):
                continue

            if df_clean[column].isna().sum() > 0:
                logger.info(f"Handling missing values for column: {column}")

                # Numerical columns
                if pd.api.types.is_numeric_dtype(df_clean[column]):
                    if column in ["credit_amount", "duration_in_month", "age_in_years"]:
                        # KNN imputation for important numeric features
                        imputer = KNNImputer(n_neighbors=5)
                        df_clean[column] = imputer.fit_transform(df_clean[[column]])[:, 0]
                    else:
                        median_val = df_clean[column].median()
                        df_clean[column].fillna(median_val, inplace=True)

                # Categorical columns
                else:
                    valid_values = self._get_valid_values_for_column(column)
                    if valid_values:
                        valid_data = df_clean[df_clean[column].isin(valid_values)][column]
                        if not valid_data.empty:
                            mode_val = valid_data.mode()
                            fill_value = mode_val[0] if not mode_val.empty else valid_values[0]
                        else:
                            fill_value = valid_values[0]
                    else:
                        mode_val = df_clean[column].mode()
                        fill_value = mode_val[0] if not mode_val.empty else "UNKNOWN"

                    df_clean[column].fillna(fill_value, inplace=True)

        return df_clean


    def _handle_outliers(self, df: pd.DataFrame, failed_columns: Set[str]) -> pd.DataFrame:
        """
        Cap or reassign outliers for numeric columns that failed range expectations.
        Focuses on upper outliers (common in corruption scenarios).

        Args:
            df: Input dataframe.
            failed_columns: Columns that failed range-based expectations.

        Returns:
            Dataframe with outliers remediated.
        """
        df_clean = df.copy()

        for column in failed_columns:
            if column not in df_clean.columns:
                continue

            if not self.should_run_remediation("expect_column_values_to_be_between", column):
                continue

            if not pd.api.types.is_numeric_dtype(df_clean[column]):
                continue

            col = df_clean[column].astype(float)
            if col.notna().sum() == 0:
                continue

            logger.info(f"Handling outliers for column: {column}")

            Q1 = col.quantile(0.25)
            Q3 = col.quantile(0.75)
            IQR = Q3 - Q1 if Q3 > Q1 else 0.0

            if IQR == 0:
                continue

            upper_bound = Q3 + 1.5 * IQR

            high_mask = (col > upper_bound) & col.notna()
            if high_mask.sum() == 0:
                continue

            normal_vals = col[~high_mask].dropna()
            if normal_vals.empty:
                df_clean.loc[high_mask, column] = upper_bound
                continue

            lower_normal = normal_vals.quantile(0.75)
            upper_normal = normal_vals.quantile(0.95)

            if lower_normal == upper_normal:
                df_clean.loc[high_mask, column] = min(upper_bound, upper_normal)
            else:
                rng = np.random.default_rng()
                corrected_vals = rng.uniform(
                    low=lower_normal,
                    high=upper_normal,
                    size=high_mask.sum(),
                )
                df_clean.loc[high_mask, column] = corrected_vals

        return df_clean


    def _handle_categorical_errors(self, df: pd.DataFrame, failed_columns: Set[str]) -> pd.DataFrame:
        """
        Fix invalid categorical values for columns failing set-based expectations.

        Args:
            df: Input dataframe.
            failed_columns: Columns that failed in-set expectations.

        Returns:
            Dataframe with invalid categories corrected.
        """
        df_clean = df.copy()

        for column in failed_columns:
            if column not in df_clean.columns:
                continue

            if pd.api.types.is_numeric_dtype(df_clean[column]):
                continue

            if self.should_run_remediation("expect_column_values_to_be_in_set", column):
                valid_values = self._get_valid_values_for_column(column)
                if valid_values:
                    invalid_mask = (
                        ~df_clean[column].isin(valid_values) & df_clean[column].notna()
                    )

                    if invalid_mask.sum() > 0:
                        logger.info(f"Fixing categorical errors for column: {column}")
                        valid_data = df_clean[df_clean[column].isin(valid_values)][column]

                        if not valid_data.empty:
                            mode_val = valid_data.mode()
                            replacement = (
                                mode_val[0] if not mode_val.empty else valid_values[0]
                            )
                        else:
                            replacement = valid_values[0]

                        df_clean.loc[invalid_mask, column] = replacement

        return df_clean



    def _handle_relationship_violations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fix business-rule violations between related columns.

        Rules currently applied:
            - credit_amount must be >= duration_in_month
            - age_in_years must be between 18 and 100

        Args:
            df: Input dataframe.

        Returns:
            Dataframe with relationship violations corrected.
        """
        df_clean = df.copy()

        # Rule: credit_amount must be >= duration_in_month
        if (
            "credit_amount" in df_clean.columns
            and "duration_in_month" in df_clean.columns
            and any(col in self.get_failed_columns() for col in ["credit_amount", "duration_in_month"])
        ):
            violation_mask = df_clean["credit_amount"] < df_clean["duration_in_month"]

            if violation_mask.sum() > 0:
                logger.info("Fixing credit_amount vs duration_in_month relationship")
                df_clean.loc[violation_mask, "credit_amount"] = (
                    df_clean.loc[violation_mask, "duration_in_month"]
                    * np.random.uniform(10, 50, violation_mask.sum())
                )

        # Rule: age_in_years must be in a realistic range
        if "age_in_years" in df_clean.columns and "age_in_years" in self.get_failed_columns():
            age_mask = (df_clean["age_in_years"] < 18) | (df_clean["age_in_years"] > 100)
            if age_mask.sum() > 0:
                logger.info("Fixing age_in_years range violations")
                reasonable_ages = df_clean.loc[
                    df_clean["age_in_years"].between(18, 100), "age_in_years"
                ]
                if not reasonable_ages.empty:
                    df_clean.loc[age_mask, "age_in_years"] = reasonable_ages.median()

        return df_clean


    def _run_advanced_techniques(
        self, df: pd.DataFrame, failed_types: Set[str], failed_columns: Set[str]
    ) -> pd.DataFrame:
        """
        Apply advanced or multi-column remediation techniques.

        Includes:
            - Multivariate outlier detection (Isolation Forest)
            - Smart categorical imputation
            - Iterative numerical imputation (MICE)

        Args:
            df: Input dataframe.
            failed_types: Failed expectation types.
            failed_columns: Columns that failed expectations.

        Returns:
            Dataframe after advanced remediation steps.
        """
        df_clean = df.copy()

        # Multivariate outliers for failed numeric columns
        numerical_failed = [
            c for c in failed_columns if c in df_clean.columns and pd.api.types.is_numeric_dtype(df_clean[c])
        ]
        if len(numerical_failed) >= 2 and any("between" in t for t in failed_types):
            logger.info("Running multivariate outlier detection")
            df_clean = self.multivariate_outlier_detection(df_clean)

        # Smart categorical imputation
        categorical_failed = [
            c for c in failed_columns if c in df_clean.columns and not pd.api.types.is_numeric_dtype(df_clean[c])
        ]
        if categorical_failed and "category" in df_clean.columns:
            logger.info("Running smart categorical imputation")
            df_clean = self.smart_categorical_imputation(df_clean)

        # Iterative imputation for multiple numeric null columns
        if any("null" in t for t in failed_types) and len(numerical_failed) >= 2:
            logger.info("Running iterative imputation for numerical columns")
            df_clean = self.iterative_imputation(df_clean, numerical_failed)

        return df_clean


    def iterative_imputation(self, df: pd.DataFrame, numeric_columns: List[str]) -> pd.DataFrame:
        """
        Perform MICE-based iterative imputation for numerical columns.

        Args:
            df: Input dataframe.
            numeric_columns: List of numeric columns to impute.

        Returns:
            Dataframe with iteratively imputed numeric values.
        """
        df_clean = df.copy()

        if numeric_columns:
            imputer = IterativeImputer(max_iter=10, random_state=42)
            df_clean[numeric_columns] = imputer.fit_transform(df_clean[numeric_columns])

        return df_clean


    def multivariate_outlier_detection(
        self, df: pd.DataFrame, contamination: float = 0.1
    ) -> pd.DataFrame:
        """
        Detect and correct multivariate outliers using Isolation Forest.
        Outliers are pulled back toward the upper-normal region.

        Args:
            df: Input dataframe.
            contamination: Expected proportion of outliers.

        Returns:
            Dataframe with multivariate outliers remediated.
        """
        df_clean = df.copy()
        numeric_columns = df_clean.select_dtypes(include=[np.number]).columns

        if len(numeric_columns) == 0:
            return df_clean

        iso_forest = IsolationForest(contamination=contamination, random_state=42)
        outliers = iso_forest.fit_predict(df_clean[numeric_columns])
        outlier_mask = outliers == -1

        if outlier_mask.sum() == 0:
            return df_clean

        logger.info(f"Multivariate outlier detection: {outlier_mask.sum()} rows flagged.")

        rng = np.random.default_rng()

        # Pull extreme outliers toward the normal range
        for column in numeric_columns:
            col = df_clean[column].astype(float)

            if col.notna().sum() == 0:
                continue

            Q1 = col.quantile(0.25)
            Q3 = col.quantile(0.75)
            IQR = Q3 - Q1 if Q3 > Q1 else 0

            if IQR == 0:
                continue

            upper_bound = Q3 + 1.5 * IQR
            high_mask = outlier_mask & (col > upper_bound) & col.notna()

            if not high_mask.any():
                continue

            normal_vals = col[~high_mask].dropna()
            if normal_vals.empty:
                df_clean.loc[high_mask, column] = upper_bound
                continue

            lower_normal = normal_vals.quantile(0.75)
            upper_normal = normal_vals.quantile(0.95)

            if lower_normal == upper_normal:
                df_clean.loc[high_mask, column] = min(upper_bound, upper_normal)
            else:
                corrected = rng.uniform(low=lower_normal, high=upper_normal, size=high_mask.sum())
                df_clean.loc[high_mask, column] = corrected

        return df_clean


    def smart_categorical_imputation(
        self, df: pd.DataFrame, target_column: str = "category"
    ) -> pd.DataFrame:
        """
        Impute missing categorical values using conditional probabilities
        based on a target variable.

        Args:
            df: Input dataframe.
            target_column: Target column used to derive conditional distributions.

        Returns:
            Dataframe with imputed categorical variables.
        """
        df_clean = df.copy()
        categorical_columns = df_clean.select_dtypes(include=["object"]).columns

        for column in categorical_columns:
            if (
                df_clean[column].isna().sum() > 0
                and target_column in df_clean.columns
                and column in self.get_failed_columns()
            ):
                temp_df = df_clean[df_clean[column].notna()]

                if not temp_df.empty:
                    prob_matrix = (
                        temp_df.groupby([column, target_column]).size().unstack(fill_value=0)
                    )
                    prob_matrix = prob_matrix.div(prob_matrix.sum(axis=1), axis=0)

                    for idx in df_clean[df_clean[column].isna()].index:
                        target_val = df_clean.loc[idx, target_column]

                        if not pd.isna(target_val):
                            best_category = prob_matrix[target_val].idxmax()
                            df_clean.loc[idx, column] = best_category
                        else:
                            mode_val = temp_df[column].mode()
                            df_clean.loc[idx, column] = mode_val[0] if not mode_val.empty else "UNKNOWN"

        return df_clean


    def _get_valid_values_for_column(self, column: str) -> list:
        """
        Retrieve valid categorical values for a column from validation rules.

        Args:
            column: Column name.

        Returns:
            List of allowed values or an empty list if not applicable.
        """
        valid_values = []

        if "expectations" in self.validation_rules:
            for expectation in self.validation_rules["expectations"]:
                if expectation.get("kwargs", {}).get("column") == column:
                    if "expect_column_values_to_be_in_set" in expectation["expectation_type"]:
                        valid_values = expectation["kwargs"].get("value_set", [])
                    elif "expect_column_values_to_be_between" in expectation["expectation_type"]:
                        return None

        return valid_values
