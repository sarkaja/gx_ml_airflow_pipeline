# include/ml_pipeline/data_preprocessor.py
from __future__ import annotations

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.pipeline import Pipeline
import logging
import os
import sys
from typing import Dict, Optional, Union
from include.dq_framework.column_utils import normalize_columns

logger = logging.getLogger("airflow.task")


def load_csv_as_df(
    csv_path: Union[str, "os.PathLike"],
    *,
    sep: str = ";",
    encoding: str = "utf-8",
    na_values=None,
    dtype: Optional[dict] = None,
    normalize_colnames: bool = True,
) -> pd.DataFrame:
    """
    Load CSV file into a DataFrame with optional column-name normalization.

    Args:
        csv_path: File path to CSV.
        sep: Field delimiter.
        encoding: File encoding.
        na_values: Additional indicators of missing values.
        dtype: Optional dtype mapping.
        normalize_colnames: Whether to normalize column names.

    Returns:
        Loaded DataFrame.
    """
    df = pd.read_csv(
        csv_path,
        sep=sep,
        encoding=encoding,
        na_values=na_values,
        dtype=dtype,
    )
    if normalize_colnames:
        df = normalize_columns(df)
    return df


class DataPreprocessor:
    """
    Preprocessing pipeline for the German Credit dataset:
    handles target creation, imputation, feature identification,
    transformation pipelines, and train/test splitting.
    """

    def __init__(self, test_size: float = 0.2, random_state: int = 42):
        """
        Initialize preprocessing configuration.

        Args:
            test_size: Test split ratio.
            random_state: Random seed.
        """
        self.test_size = test_size
        self.random_state = random_state
        self.preprocessor = None
        self.categorical_columns = None
        self.numerical_columns = None

    def prepare_target(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
        """
        Convert the 'category' column into a binary target and extract features.

        Mapping:
            1 → 0 (good)
            2 → 1 (bad)

        Args:
            df: Input DataFrame containing a 'category' column.

        Returns:
            (X, y) tuple of features and target.

        Raises:
            ValueError: If the category column is missing or invalid.
        """
        if "category" not in df.columns:
            raise ValueError("DataFrame does not contain 'category' column")

        df = df.copy()
        df["target"] = df["category"].map({1: 0, 2: 1})

        if df["target"].isna().any():
            raise ValueError("Target contains values other than 1/2")

        X = df.drop(columns=["category", "target"])
        y = df["target"]

        logger.info(f"Target distribution: {y.value_counts().to_dict()}")
        logger.info(f"Features shape: {X.shape}")

        return X, y

    def basic_imputation(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Perform simple imputation for missing values.

        - Numeric → median
        - Categorical → mode or 'UNKNOWN'

        Args:
            X: Features DataFrame.

        Returns:
            DataFrame with missing values imputed.
        """
        X_imputed = X.copy()

        # Numeric columns → median
        numeric_cols = X.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if X[col].isna().any():
                X_imputed[col] = X[col].fillna(X[col].median())

        # Categorical columns → mode
        categorical_cols = X.select_dtypes(include=['object']).columns
        for col in categorical_cols:
            if X[col].isna().any():
                if not X[col].mode().empty:
                    X_imputed[col] = X[col].fillna(X[col].mode()[0])
                else:
                    X_imputed[col] = X[col].fillna("UNKNOWN")

        return X_imputed

    def identify_feature_types(self, X: pd.DataFrame):
        """
        Identify which columns are categorical or numerical.

        Args:
            X: Feature dataset.
        """
        self.categorical_columns = X.select_dtypes(include=["object", "string"]).columns.tolist()
        self.numerical_columns = X.select_dtypes(exclude=["object", "string"]).columns.tolist()

        logger.info(f"Categorical columns: {self.categorical_columns}")
        logger.info(f"Numerical columns: {self.numerical_columns}")

    def create_preprocessor(self) -> ColumnTransformer:
        """
        Build a ColumnTransformer with StandardScaler for numeric
        and OneHotEncoder for categorical features.

        Returns:
            Configured ColumnTransformer.

        Raises:
            ValueError: If feature types have not been identified.
        """
        if self.categorical_columns is None or self.numerical_columns is None:
            raise ValueError("Feature types not identified. Call identify_feature_types first.")

        self.preprocessor = ColumnTransformer(
            transformers=[
                ("num", StandardScaler(), self.numerical_columns),
                ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), self.categorical_columns),
            ],
            remainder="drop",
        )
        return self.preprocessor

    def split_data(self, X: pd.DataFrame, y: pd.Series) -> tuple:
        """
        Split dataset into training and testing subsets with stratification.

        Args:
            X: Feature matrix.
            y: Target vector.

        Returns:
            (X_train, X_test, y_train, y_test)
        """
        x_train, x_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=self.test_size,
            random_state=self.random_state,
            stratify=y,
        )

        logger.info(f"Train set: {x_train.shape}, Test set: {x_test.shape}")
        return x_train, x_test, y_train, y_test
