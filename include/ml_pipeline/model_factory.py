# include/ml_pipeline/model_factory.py
from __future__ import annotations

from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.pipeline import Pipeline
from typing import Dict, Any

class ModelFactory:
    """
    Factory for creating ML models with preprocessing pipelines.
    """
    
    @staticmethod
    def create_logistic_regression(preprocessor, **kwargs) -> Pipeline:
        """
        Create logistic regression pipeline.
        
        Args:
            preprocessor: Feature preprocessor
            **kwargs: Additional parameters for LogisticRegression
            
        Returns:
            Configured pipeline
        """
        default_params = {
            "max_iter": 2000,
            "class_weight": "balanced",
            "random_state": 42
        }
        default_params.update(kwargs)
        
        return Pipeline(steps=[
            ("preprocessor", preprocessor),
            ("classifier", LogisticRegression(**default_params))
        ])
    
    @staticmethod
    def create_random_forest(preprocessor, **kwargs) -> Pipeline:
        """
        Create random forest pipeline.
        
        Args:
            preprocessor: Feature preprocessor
            **kwargs: Additional parameters for RandomForestClassifier
            
        Returns:
            Configured pipeline
        """
        default_params = {
            "n_estimators": 500,
            "max_depth": None,
            "min_samples_split": 2,
            "min_samples_leaf": 1,
            "class_weight": "balanced",
            "random_state": 42,
            "n_jobs": -1
        }
        default_params.update(kwargs)
        
        return Pipeline(steps=[
            ("preprocessor", preprocessor),
            ("classifier", RandomForestClassifier(**default_params))
        ])
    
    @staticmethod
    def get_available_models() -> Dict[str, Any]:
        """
        Get list of available model types.
        
        Returns:
            Dictionary of model names and their creation methods
        """
        return {
            "logistic_regression": ModelFactory.create_logistic_regression,
            "random_forest": ModelFactory.create_random_forest
        }