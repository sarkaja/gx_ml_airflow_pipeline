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
from include.scripts.setup_environment import setup_environment
from include.dq_framework.column_utils import normalize_columns

PROJECT_DIR = setup_environment()
if PROJECT_DIR not in sys.path:
    sys.path.append(PROJECT_DIR)

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
    """Load CSV file into DataFrame with optional column normalization."""
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
    Data preprocessing for German Credit dataset.
    """
    
    def __init__(self, test_size: float = 0.2, random_state: int = 42):
        """
        Initialize data preprocessor.
        
        Args:
            test_size: Proportion of test set
            random_state: Random seed for reproducibility
        """
        self.test_size = test_size
        self.random_state = random_state
        self.preprocessor = None
        self.categorical_columns = None
        self.numerical_columns = None

        


    
    def prepare_target(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
        """
        Prepare target variable from category column.
        
        Args:
            df: Input DataFrame with 'category' column
            
        Returns:
            Tuple of (features, target)
            
        Raises:
            ValueError: If category column is missing or contains invalid values
        """
        if "category" not in df.columns:
            raise ValueError("DataFrame does not contain 'category' column")
        
        # 1 = good, 2 = bad -> 0 = good, 1 = bad
        df = df.copy()
        df["target"] = df["category"].map({1: 0, 2: 1})
        
        if df["target"].isna().any():
            raise ValueError("Target contains values other than 1/2")
        
        X = df.drop(columns=["category", "target"])
        y = df["target"]
        
        logger.info(f"Target distribution: {y.value_counts().to_dict()}")
        logger.info(f"Features shape: {X.shape}")
        
        return X, y
    
    def identify_feature_types(self, X: pd.DataFrame):
        """
        Identify categorical and numerical columns.
        
        Args:
            X: Features DataFrame
        """
        self.categorical_columns = X.select_dtypes(include=["object", "string"]).columns.tolist()
        self.numerical_columns = X.select_dtypes(exclude=["object", "string"]).columns.tolist()
        
        logger.info(f"Categorical columns: {self.categorical_columns}")
        logger.info(f"Numerical columns: {self.numerical_columns}")
    
    def create_preprocessor(self) -> ColumnTransformer:
        """
        Create preprocessing pipeline for features.
        
        Returns:
            Configured ColumnTransformer
        """
        if self.categorical_columns is None or self.numerical_columns is None:
            raise ValueError("Feature types not identified. Call identify_feature_types first.")
        
        self.preprocessor = ColumnTransformer(
            transformers=[
                ("num", StandardScaler(), self.numerical_columns),
                ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), self.categorical_columns),
            ],
            remainder="drop"
        )
        
        return self.preprocessor
    
    def split_data(self, X: pd.DataFrame, y: pd.Series) -> tuple:
        """
        Split data into train and test sets.
        
        Args:
            X: Features
            y: Target
            
        Returns:
            Tuple of (x_train, x_test, y_train, y_test)
        """
        x_train, x_test, y_train, y_test = train_test_split(
            X, y,
            test_size=self.test_size,
            random_state=self.random_state,
            stratify=y
        )
        
        logger.info(f"Train set: {x_train.shape}, Test set: {x_test.shape}")
        return x_train, x_test, y_train, y_test